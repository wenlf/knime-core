package org.knime.core.data.table.cache;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchWriter;
import org.knime.core.data.table.store.TableStore;

/*
 TODO interface for cache
 TODO async pre-load here or in the actual cursor?
 TODO thread-safety
*/
public class CachedTableStore implements TableStore, Flushable {

	// one cache for each column. use-case: two tables with different filters access
	// same table.
	private List<Map<Integer, ColumnChunk>> m_caches;

	// all types
	private ColumnType<?, ?>[] m_types;

	// writer
	private RowBatchWriter m_writer;

	// size of cache
	private int m_numChunks = 0;
	private int m_flushIndex = 0;

	private TableStore m_delegate;
	private CachedTableReadStore m_readCache;
	private boolean m_finishedWriting = false;

	public CachedTableStore(final TableStore delegate) {
		m_types = delegate.getColumnTypes();
		m_delegate = delegate;
		m_caches = new ArrayList<>();
		for (int i = 0; i < m_types.length; i++) {
			m_caches.add(new TreeMap<Integer, ColumnChunk>());
		}

		// read store.
		m_readCache = new CachedTableReadStore(delegate, m_caches);

		// Only one writer. Maybe a property of the delegate
		m_writer = delegate.getWriter();
	}

	@Override
	public RowBatchWriter getWriter() {
		// TODO make sure we only have one writer for now.
		// Write each column individually in the corresponding cache
		return new RowBatchWriter() {

			@Override
			public void write(final RowBatch data) {
				final ColumnChunk[] columnData = data.getRecordData();
				for (int i = 0; i < m_types.length; i++) {
					columnData[i].retain();
					m_caches.get(i).put(m_numChunks, columnData[i]);
				}
				m_numChunks++;
				m_readCache.incNumChunks();
			}

			@Override
			public void close() throws Exception {
				m_finishedWriting = true;
			}
		};
	}

	/**
	 * BIG TODO: implement flush differently for read-only scenario!!!!
	 */
	@Override
	public void flush() throws IOException {
		final ColumnChunk[] data = new ColumnChunk[m_caches.size()];
		// TODO always flush fully?
		// TODO for async we have to refactor the while-loop
		for (; m_flushIndex < m_numChunks; m_flushIndex++) {
			for (int i = 0; i < m_caches.size(); i++) {
				final Map<Integer, ColumnChunk> cache = m_caches.get(i);
				data[i] = cache.get(m_flushIndex);
			}
			// TODO reuse existing record object to avoid re-creation. later.
			m_writer.write(new DefaultRowBatch(data));

			// for each column cache
			for (int i = 0; i < data.length; i++) {
				data[i].release();
				m_caches.get(i).clear();
			}
		}
		if (m_flushIndex == m_numChunks & m_finishedWriting) {
			try {
				m_writer.close();
			} catch (Exception e) {
				// TODO OK?
				throw new IOException(e);
			}
		}
		// NB: keep caches open.
	}

	// Move to CachedRecordReadStore. Make sure caches are 'lazily' instantiated in
	// case of read access.
	@Override
	public RowBatchReader createReader() {
		return m_readCache.createReader();
	}

	@Override
	public ColumnType<?, ?>[] getColumnTypes() {
		return m_types;
	}

	@Override
	public void close() throws Exception {
		m_readCache.clear();
		m_delegate.close();
	}

	@Override
	public RowBatchFactory createFactory() {
		// TODO anything smart we can do here? Track created data? reuse data? etc?
		// Later...
		return m_delegate.createFactory();
	}
}
