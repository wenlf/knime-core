package org.knime.core.data.arrow;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowUtils;
import org.knime.core.data.row.RowUtils.ValueRange;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.type.DateTimeType;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringType;
import org.knime.core.data.value.DateTimeReadValue;
import org.knime.core.data.value.DateTimeWriteValue;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;
import org.knime.core.data.value.StringReadValue;
import org.knime.core.data.value.StringWriteValue;

public class MixedTypesTest extends AbstractArrowTest {

	@Test
	public void identityTestFourColumns() throws Exception {
		final int chunkSize = 17;
		final int numRows = 1702;

		try (final TableStore store = cache(createStore(chunkSize, DoubleType.INSTANCE, DoubleType.INSTANCE,
				StringType.INSTANCE, DateTimeType.INSTANCE))) {
			final WriteTable writeTable = TableUtils.createWriteTable(store);
			try (RowWriteCursor writeCursor = writeTable.getCursor()) {
				final ValueRange<DoubleWriteValue> doubleRange = RowUtils.getRange(writeCursor, 0, 2);
				final StringWriteValue stringWriteValue = writeCursor.get(2);
				final DateTimeWriteValue dateTimeWriteValue = writeCursor.get(3);

				for (int i = 0; i < numRows; i++) {
					writeCursor.fwd();
					stringWriteValue.setStringValue("Entry" + i);
					dateTimeWriteValue.setDateTime(i, i + 1);
					if (i % 100 == 0) {
						for (int j = 0; j < 2; j++) {
							doubleRange.get(j).setMissing();
						}
					} else {
						for (int j = 0; j < 2; j++) {
							doubleRange.get(j).setDouble(i + j);
						}
					}
				}
			}

			final ReadTable readTable = TableUtils.createReadTable(store);
			try (RowReadCursor readCursor = readTable.newCursor()) {
				final ValueRange<DoubleReadValue> doubleRange = RowUtils.getRange(readCursor, 0, 2);
				final StringReadValue stringReadValue = readCursor.get(2);
				final DateTimeReadValue dateTimeReadValue = readCursor.get(3);

				for (int i = 0; i < numRows; i++) {
					readCursor.fwd();
					Assert.assertEquals("Entry" + i, stringReadValue.getStringValue());
					Assert.assertEquals(i, dateTimeReadValue.getDate(), 0.000001);
					Assert.assertEquals(i + 1, dateTimeReadValue.getTime(), 0.000001);

					if (i % 100 == 0) {
						for (int j = 0; j < 2; j++) {
							Assert.assertTrue(doubleRange.get(j).isMissing());
						}
					} else {
						for (int j = 0; j < 2; j++) {
							Assert.assertEquals(i + j, doubleRange.get(j).getDouble(), 0.000001);
						}
					}
				}
			}
		}
	}
}
