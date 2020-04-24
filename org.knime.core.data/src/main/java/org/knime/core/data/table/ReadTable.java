package org.knime.core.data.table;

import org.knime.core.data.row.RowReadCursor;

public interface ReadTable {
	long getNumColumns();

	RowReadCursor newCursor();
}
