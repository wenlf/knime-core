package org.knime.core.data.column.struct;

import org.knime.core.data.column.ColumnChunk;

public interface StructChunk extends ColumnChunk {
	ColumnChunk getColumnChunk(int index);
}
