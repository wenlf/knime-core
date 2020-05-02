package org.knime.core.data.table.store;

import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.RowBatchFactory;

public interface TableStore extends TableWriteStore, TableReadStore, AutoCloseable {
	ColumnType<?, ?>[] getColumnTypes();

	RowBatchFactory createFactory();
}
