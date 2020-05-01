package org.knime.core.data.table.store;

import java.io.File;

import org.knime.core.data.column.ColumnType;

public interface TableStoreFactory extends AutoCloseable {

	TableStore create(ColumnType<?, ?>[] schema, File file, TableStoreConfig config);

	TableReadStore create(ColumnType<?, ?>[] schema, File file, long size);

}
