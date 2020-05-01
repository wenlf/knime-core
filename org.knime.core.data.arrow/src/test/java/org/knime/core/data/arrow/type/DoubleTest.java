package org.knime.core.data.arrow.type;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.arrow.AbstractArrowTest;
import org.knime.core.data.preproc.PreProcTableStore;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreUtils;
import org.knime.core.data.type.DoubleAccess;
import org.knime.core.data.type.DoubleDomain;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;

public class DoubleTest extends AbstractArrowTest {

	@Test
	public void float8VectorChunkTest() {
		final int chunkSize = 5;
		try (BufferAllocator allocator = newAllocator()) {
			Float8VectorChunk data = new Float8VectorChunk(allocator);
			data.allocateNew(chunkSize);
			DoubleAccess access = new DoubleAccess();
			access.load(data);
			access.reset();

			int tmp = 0;

			DoubleWriteValue write = access;
			for (int i = 0; i < chunkSize; i++) {
				access.fwd();
				write.setDouble(i);
				tmp++;
			}

			assertEquals(chunkSize, tmp);
			data.setNumValues(chunkSize);
			access.reset();
			tmp = 0;

			DoubleReadValue read = access;
			for (int i = 0; i < data.getNumValues(); i++) {
				access.fwd();
				assertEquals(i, read.getDouble(), 0.0000000000001);
				tmp++;
			}
			assertEquals(chunkSize, tmp);
			data.release();
		}
	}

	@Test
	public void identityTestSingleColumn() throws Exception {
		final int chunkSize = 17;
		final int numRows = 1702;

		try (final TableStore store = cache(createStore(chunkSize, DoubleType.INSTANCE))) {
			final WriteTable writeTable = TableStoreUtils.createWriteTable(store);
			try (RowWriteCursor writeCursor = writeTable.getCursor()) {
				final DoubleWriteValue doubleWriteValue = writeCursor.get(0);
				for (int i = 0; i < numRows; i++) {
					writeCursor.fwd();
					if (i % 100 == 0) {
						doubleWriteValue.setMissing();
					} else {
						doubleWriteValue.setDouble(i);
					}
				}
			}
			final ReadTable readTable = TableStoreUtils.createReadTable(store);
			try (RowReadCursor readCursor = readTable.newCursor()) {
				DoubleReadValue doubleReadValue = readCursor.get(0);
				for (int i = 0; i < numRows; i++) {
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

	@Test
	public void testDomain() throws Exception {
		final int chunkSize = 10000;
		final int numRows = 10000000;

		try (final PreProcTableStore store = preproc(
				cache(createStore(chunkSize, DoubleType.INSTANCE, DoubleType.INSTANCE)), 0, 1)) {
			final WriteTable writeTable = TableStoreUtils.createWriteTable(store);
			try (RowWriteCursor writeCursor = writeTable.getCursor()) {
				final DoubleWriteValue doubleWriteValue = writeCursor.get(0);
				for (int i = 0; i < numRows; i++) {
					writeCursor.fwd();
					if (i % 2 == 0) {
						doubleWriteValue.setMissing();
					} else {
						doubleWriteValue.setDouble(i);
					}
				}
			}

			final DoubleDomain domain = store.getResultDomain(0);
			assertEquals(numRows / 2, domain.getNumMissing());
			assertEquals(numRows / 2, domain.getNumNonMissing());
			assertEquals(numRows - 1, domain.getMaximum(), 0.000000000000001);
			assertEquals(1, domain.getMinimum(), 0.000000000000001);
		}
	}
}
