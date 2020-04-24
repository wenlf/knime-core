package org.knime.core.data.table.store;

import java.io.File;

import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.RowBatchFactory;

public interface TableStoreFactory extends AutoCloseable {

	// TODO move into store.
	RowBatchFactory createFactory(final ColumnType<?, ?>[] schema);

	TableStore create(ColumnType<?, ?>[] schema, File file, TableStoreConfig hints);

}
