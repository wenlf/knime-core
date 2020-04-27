package org.knime.core.data.arrow;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.cache.CachedTableStore;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;

public class CacheTest extends AbstractArrowTest {
	@Test
	public void testFlush() throws Exception {
		final int numRows = 100000;
		final int chunkSize = 17;

		try (CachedTableStore cache = cache(createStore(chunkSize, DoubleType.INSTANCE))) {
			// TODO create writer hints (e.g. dictionary encoding for column X)
			final WriteTable writeTable = TableUtils.createWriteTable(cache);
			try (RowWriteCursor writeCursor = writeTable.getCursor()) {
				final DoubleWriteValue doubleWriteValue = (DoubleWriteValue) writeCursor.get(0);
				for (int i = 0; i < numRows; i++) {
					writeCursor.fwd();

					// Flushing sometimes.
					if (i % 17 == 0) {
						cache.flush();
					}

					if (i % 100 == 0) {
						doubleWriteValue.setMissing();
					} else {
						doubleWriteValue.setDouble(i);
					}
				}
			}

			/*
			 * If we had to flush the table while writing, we have to fully flush the table
			 * before reading it again.
			 * 
			 * Simple reason: We can't read from a (random-accessible) file which still has
			 * an open writer stream. Possible solutions:
			 * 
			 * - (1) if Parquet supports reading while writing we're fine.
			 * 
			 * - (2) We've to write multiple files for very large tables.
			 * 
			 * TODO we don't have to clear the cache in case we just flush because we have
			 * to close the file.
			 */
			cache.flush();

			final ReadTable readTable = TableUtils.createReadTable(cache);
			try (RowReadCursor readCursor = readTable.newCursor()) {
				DoubleReadValue doubleReadValue = (DoubleReadValue) readCursor.get(0);
				for (int i = 0; i < 5; i++) {
					readCursor.fwd();
					if (i % 100 == 0) {
						Assert.assertTrue(doubleReadValue.isMissing());
					} else {
						assertEquals(i, doubleReadValue.getDouble(), 0.00000000000000001);
					}
				}
			}
		}
	}
}
