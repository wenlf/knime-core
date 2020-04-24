package org.knime.core.data.row;

import org.knime.core.data.ChunkFactory;
import org.knime.core.data.column.ColumnType;

public interface RowBatchFactory extends ChunkFactory<RowBatch> {
	ColumnType<?, ?>[] getColumnTypes();
}
