package org.knime.core.data.table.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.table.store.TableReadStore;

// TODO interface for cache
// TODO async pre flush?
// TODO thread-safety
public class CachedTableReadStore implements TableReadStore {

	// one cache for each column. use-case: two tables with different filters access
	// same table.
	private List<Map<Integer, ColumnChunk>> m_caches;

	// size of cache
	private int m_numChunks = 0;

	private ColumnType<?, ?>[] m_types;

	private TableReadStore m_delegate;

	CachedTableReadStore(final TableReadStore delegate, final List<Map<Integer, ColumnChunk>> caches) {
		m_caches = caches;

		// TODO we create more readers for parallel reading.
		m_types = delegate.getColumnTypes();
		m_delegate = delegate;
	}

	public CachedTableReadStore(final TableReadStore delegate) {
		this(delegate, initializeEmptyCaches(delegate.getColumnTypes().length));
	}

	private static List<Map<Integer, ColumnChunk>> initializeEmptyCaches(int numCaches) {
		final ArrayList<Map<Integer, ColumnChunk>> caches = new ArrayList<>();
		for (int i = 0; i < numCaches; i++) {
			caches.add(new TreeMap<Integer, ColumnChunk>());
		}
		return caches;
	}

	// TODO we have to set this from outside when loading the store for read-only
	void incNumChunks() {
		m_numChunks++;
	}

	// TODO interface?
	public void clear() throws IOException {
		for (int j = 0; j < m_caches.size(); j++) {
			final Map<Integer, ColumnChunk> cache = m_caches.get(j);
			for (final ColumnChunk data : cache.values()) {
				data.release();
			}
			cache.clear();
		}
		// NB: keep caches open
	}

	// Move to CachedRecordReadStore. Make sure caches are 'lazily' instantiated in
	// case of read access.
	@Override
	public RowBatchReader createReader(RowBatchReaderConfig config) {

		// create new reader per read request. each reader could be configured
		// differently. configuration constant per reader.
		final RowBatchReader reader = m_delegate.createReader(config);

		// TODO we have to pass the config
		return new RowBatchReader() {

			@Override
			public int getNumChunks() {
				return m_numChunks;
			}

			// TODO don't create a new object each time. reuse and update.
			// TODO thread-safety. what happens if after columnData.get() the data is
			// flushed and released?

			@Override
			public RowBatch read(int chunkIndex) {
				final int[] indices = config.getColumnIndices();
				final boolean isSelection = indices != null;
				final int numRequested = isSelection ? indices.length : m_caches.size();
				final ColumnChunk[] data = new ColumnChunk[numRequested];
				final BitSet bits = new BitSet(data.length);

				// TODO if indices == null, we can simply set ALL bits or just move an and
				// request all. Can be much more efficiently implemeneted than what I did here.
				for (int i = 0; i < numRequested; i++) {
					final ColumnChunk columnData = m_caches.get(isSelection ? indices[i] : i).get(chunkIndex);
					if (columnData == null) {
						bits.clear(i);
					} else {
						data[i] = columnData;
						bits.set(i);
					}
				}
				/*
				 * TODO Now we could be nice to our friends also reading from this cache and
				 * also load their data. Optimizes away additional IO call overhead to backend.
				 * Implementation idea (1): keep list of readers and check which reader is close
				 * behind us. Implementation idea (2): for each reader at each chunk index keep
				 * the superset of data which will likely be read at some point. Obviously we
				 * don't want to keep objects for each index, so we have to be smarter somehow.
				 * 
				 * Read-case should should work as follows: Check what's in the cache and retain
				 * available data. Create new RecordBatchReader for missing data. Depending on
				 * some heuristics we can also read-in all requested columns in following
				 * requests. -- (Counter example: Cursor A requires 1 column, Cursor B
				 * 2,3,4,5,6,7,8... Cursor A is at chunk index 1, Cursor B at size-1. We should
				 * only read in 1,2,3,4,5,6,7,8 for the last request of Cursor B. It's a union
				 * of columns for each chunk index somehow.
				 */
				final int numMissing = numRequested - bits.cardinality();
				final RowBatch out;
				if (numMissing == 0) {
					out = new DefaultRowBatch(data);
				} else if (numMissing == numRequested) {
					final RowBatch record = request(chunkIndex, config);
					final ColumnChunk[] readData = record.getRecordData();
					for (int i = 0; i < data.length; i++) {
						m_caches.get(i).put(chunkIndex, readData[i]);
					}
					out = record;
				} else {
					final int[] missing = new int[numMissing];
					int next = -1;
					for (int i = 0; i < numMissing; i++) {
						next = bits.nextClearBit(next + 1);
						missing[i] = next;
					}
					// subset of data to be stored.
					final RowBatch tmp = request(chunkIndex, new RowBatchReaderConfig() {

						public int[] getColumnIndices() {
							return missing;
						}
					});
					for (int d = 0; d < missing.length; d++) {
						data[missing[d]] = tmp.getRecordData()[d];
					}
					out = new DefaultRowBatch(data);
				}

				out.retain();
				return out;
			}

			// temporary solution. do we want the actual readers to live 'outside'?
			private RowBatch request(int chunkIndex, RowBatchReaderConfig hints) {
				// TODO smarter! more reader threads? pre-loading etc.
				return reader.read(chunkIndex);
			}

			@Override
			public void close() throws Exception {
				reader.close();
			}
		};
	}

	@Override
	public ColumnType<?, ?>[] getColumnTypes() {
		return m_types;
	}

	@Override
	public void close() throws Exception {
		clear();
	}

	@Override
	public long size() {
		return m_delegate.size();
	}
}
