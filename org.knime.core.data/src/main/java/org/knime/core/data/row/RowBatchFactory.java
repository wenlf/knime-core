package org.knime.core.data.row;

import org.knime.core.data.column.ColumnType;

public interface RowBatchFactory {
	ColumnType<?, ?>[] getColumnTypes();

	RowBatch create();
}
