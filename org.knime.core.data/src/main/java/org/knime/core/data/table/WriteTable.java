package org.knime.core.data.table;

import org.knime.core.data.row.RowWriteCursor;

public interface WriteTable {

	int getNumColumns();

	// TODO discuss: returns single write cursor (therefore the name 'get').
	// If we want to support parallel writes, we would have to create multiple
	// cursors here. For now only single write into cache.
	// NB: if we support multiple write cursor on cache we likely have to decouple
	// cache from IO/in-memory back-end further, as most formats don't support
	// parallel writes (some formats support splitted files which can then be
	// written in parallel though, e.g. parquet, n5, arrow etc).
	RowWriteCursor getCursor();
}
