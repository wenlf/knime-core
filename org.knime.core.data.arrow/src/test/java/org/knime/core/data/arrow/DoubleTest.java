package org.knime.core.data.arrow;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.row.ReadValueRange;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.row.ValueRange;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.type.DoubleAccess;
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
	public void identityTestWideTable() throws Exception {
		final int chunkSize = 2000;
		final int numRows = 10000;
		final int numColumns = 30000;

		try (final TableStore store = cache(
				createStore(chunkSize, createWideSchema(DoubleType.INSTANCE, numColumns)))) {
			final WriteTable writeTable = TableUtils.createWriteTable(store);
			try (RowWriteCursor writeCursor = writeTable.getCursor()) {
				final ValueRange<DoubleWriteValue> doubleWriteValue = writeCursor.getRange(0, numColumns);
				for (int i = 0; i < numRows; i++) {
					writeCursor.fwd();
					if (i % 100 == 0) {
						for (int j = 0; j < numColumns; j++) {
							doubleWriteValue.getWriteValue(j).setMissing();
						}
					} else {
						for (int j = 0; j < numColumns; j++) {
							doubleWriteValue.getWriteValue(j).setDouble(i + j);
						}
					}
				}
			}
			final ReadTable readTable = TableUtils.createReadTable(store);
			try (RowReadCursor readCursor = readTable.newCursor()) {
				final ReadValueRange<DoubleReadValue> doubleReadValue = readCursor.getRange(0, numColumns);
				for (int i = 0; i < numRows; i++) {
					readCursor.fwd();
					if (i % 100 == 0) {
						for (int j = 0; j < numColumns; j++) {
							Assert.assertTrue(doubleReadValue.getReadValue(j).isMissing());
						}
					} else {
						for (int j = 0; j < numColumns; j++) {
							assertEquals(i + j, doubleReadValue.getReadValue(j).getDouble(), 0.00000000000000001);
						}
					}
				}
			}
		}
	}

	@Test
	public void identityTestSingleColumn() throws Exception {
		final int chunkSize = 17;
		final int numRows = 1702;

		try (final TableStore store = cache(createStore(chunkSize, DoubleType.INSTANCE))) {
			final WriteTable writeTable = TableUtils.createWriteTable(store);
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
			final ReadTable readTable = TableUtils.createReadTable(store);
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
}
