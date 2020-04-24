package org.knime.core.data.table.store;

import org.knime.core.data.column.ColumnType;

public interface TableStore extends TableWriteStore, TableReadStore, AutoCloseable {
	ColumnType<?, ?>[] getColumnTypes();

	// TODO add factory from Factory
}
