package org.knime.core.data.table.store;

import org.knime.core.data.row.RowBatchReader;

public interface TableReadStore {
	RowBatchReader createReader();
}
