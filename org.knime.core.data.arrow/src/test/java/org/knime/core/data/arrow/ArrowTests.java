
package org.knime.core.data.arrow;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowBatchUtils;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.cache.CachedTableStore;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreFactory;
import org.knime.core.data.type.DoubleAccess;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;

// TODO share tests among all backends.
public class ArrowTests {

	/**
	 * Some variables
	 */
	// in numValues per vector
	public static final int RECORDBATCH_SIZE = 25;

	// format
	public static final TableStoreFactory FORMAT = new ArrowTableStoreFactory(RECORDBATCH_SIZE);

	// in bytes
	public static final long OFFHEAP_SIZE = 2000_000_000;

	// num rows used for testing
	public static final long NUM_ROWS = 1_000_000_000;

	// some schema
	private static final ColumnType<?, ?>[] SCHEMA = new ColumnType[] { DoubleType.INSTANCE };

	/**
	 * TESTS
	 * 
	 * @throws Exception
	 */
	@AfterClass
	public static void shutdown() throws Exception {
		FORMAT.close();
	}

	@Test
	public void doubleData() {
		final RootAllocator root = new RootAllocator();

		Float8VectorChunk data = new Float8VectorChunk(root);
		data.allocateNew(3);
		DoubleAccess access = new DoubleAccess();
		access.update(data);

		int tmp = 0;

		DoubleWriteValue write = access;
		for (int i = 0; i < data.getMaxCapacity(); i++) {
			access.fwd();
			write.setDouble(i);
			tmp++;
		}

		assertEquals(3, tmp);
		data.setNumValues(3);
		access.reset();
		tmp = 0;

		DoubleReadValue read = access;
		for (int i = 0; i < data.getNumValues(); i++) {
			access.fwd();
			assertEquals(i, read.getDouble(), 0.0000000000001);
			tmp++;
		}

		assertEquals(3, tmp);
	}

	@Test
	public void identityTestReadWrite() throws Exception {
		try (TableStore store = FORMAT.create(SCHEMA, createTmpFile(), null)) {
			identityTestSingleDoubleColumn(store);
		}
	}

	@Test
	public void identityTestReadWriteCache() throws Exception {
		try (CachedTableStore store = RowBatchUtils.cache(FORMAT.create(SCHEMA, createTmpFile(), null))) {
			identityTestSingleDoubleColumn(store);
		}
	}

	@Test
	public void testFlush() throws Exception {

		try (CachedTableStore cache = RowBatchUtils.cache(FORMAT.create(SCHEMA, createTmpFile(), null))) {
			// TODO create writer hints (e.g. dictionary encoding for column X)
			final WriteTable writeTable = TableUtils.createWriteTable(cache);
			try (RowWriteCursor writeCursor = writeTable.getCursor()) {
				final DoubleWriteValue doubleWriteValue = (DoubleWriteValue) writeCursor.get(0);
				for (int i = 0; i < NUM_ROWS; i++) {
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

			final ReadTable readTable = TableUtils.createReadTable(SCHEMA, cache, new RowBatchReaderConfig() {

				@Override
				public int[] getColumnIndices() {
					return new int[] { 0 };
				}
			});
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

	private File createTmpFile() throws IOException {
		// file
		final File f = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow").toFile();
		f.deleteOnExit();
		return f;
	}

	private void identityTestSingleDoubleColumn(TableStore store) throws Exception {
		// TODO create writer hints (e.g. dictionary encoding for column X)
		final WriteTable writeTable = TableUtils.createWriteTable(store);
		try (RowWriteCursor writeCursor = writeTable.getCursor()) {
			final DoubleWriteValue doubleWriteValue = (DoubleWriteValue) writeCursor.get(0);
			for (int i = 0; i < 5; i++) {
				writeCursor.fwd();
				if (i % 100 == 0) {
					doubleWriteValue.setMissing();
				} else {
					doubleWriteValue.setDouble(i);
				}
			}
		}
		// TODO create reader hints factory or object or builder or.. (e.g. start
		// reading at row-index X or only read
		// columns 1,2, etc).
		final ReadTable readTable = TableUtils.createReadTable(SCHEMA, store, new RowBatchReaderConfig() {

			@Override
			public int[] getColumnIndices() {
				return new int[] { 0 };
			}
		});
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
