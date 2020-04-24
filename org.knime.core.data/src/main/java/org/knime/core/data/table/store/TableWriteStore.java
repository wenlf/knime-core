package org.knime.core.data.table.store;

import org.knime.core.data.row.RowBatchWriter;

public interface TableWriteStore {
	RowBatchWriter getWriter();
}
