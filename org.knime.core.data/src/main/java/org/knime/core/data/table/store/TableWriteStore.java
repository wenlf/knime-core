package org.knime.core.data.table.store;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.row.RowBatchWriter;

public interface TableWriteStore {
	RowBatchWriter getWriter();

	void copyDataTo(File file) throws IOException;
}
