package org.knime.core.data.row;

import org.knime.core.data.Chunk;
import org.knime.core.data.column.ColumnChunk;

public interface RowBatch extends Chunk {
	ColumnChunk[] getRecordData();
}
