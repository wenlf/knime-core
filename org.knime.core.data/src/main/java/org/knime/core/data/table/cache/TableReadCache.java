package org.knime.core.data.table.cache;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.table.store.TableReadStore;

// TODO interface for cache
// TODO async pre flush?
// TODO thread-safety
public class TableReadCache implements TableReadStore, AutoCloseable {

	// one cache for each column. use-case: two tables with different filters access
	// same table.
	private List<Map<Integer, ColumnChunk>> m_caches;

	// size of cache
	private int m_numChunks = 0;

	private RowBatchReader m_reader;

	TableReadCache(final TableReadStore delegate, final List<Map<Integer, ColumnChunk>> caches) {
		m_caches = caches;

		// TODO we create more readers for parallel reading.
		m_reader = delegate.createReader();
	}

	// increment the number of chunks by one (in case we're currently still
	// writing).

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
	public RowBatchReader createReader() {
		return new RowBatchReader() {

			@Override
			public int getNumChunks() {
				return m_numChunks;
			}

			// TODO don't create a new object each time. reuse and update.
			// TODO thread-safety. what happens if after columnData.get() the data is
			// flushed and released?

			@Override
			public RowBatch read(int chunkIndex, RowBatchReaderConfig hints) {
				// TODO special case when we know entire table is in cache

				final int[] indices = hints.getColumnIndices();
				final ColumnChunk[] data = new ColumnChunk[indices.length];
				final BitSet bits = new BitSet(data.length);
				for (int i = 0; i < indices.length; i++) {
					final ColumnChunk columnData = m_caches.get(indices[i]).get(chunkIndex);
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

				final int numMissing = indices.length - bits.cardinality();
				final RowBatch out;
				if (numMissing == 0) {
					out = new DefaultRowBatch(data);
				} else if (numMissing == indices.length) {
					final RowBatch record = request(chunkIndex, hints);
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
				return m_reader.read(chunkIndex, hints);
			}

			@Override
			public void close() throws Exception {
				m_reader.close();
			}
		};
	}

	@Override
	public void close() throws Exception {
		clear();
	}
}
