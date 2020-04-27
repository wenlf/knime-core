package org.knime.core.data.table.store;

import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;

public interface TableReadStore {
	RowBatchReader createReader(RowBatchReaderConfig config);

	ColumnType<?, ?>[] getColumnSpec();
}
