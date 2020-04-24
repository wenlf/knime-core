package org.knime.core.data.column.struct;

import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;

public interface StructChunk extends ColumnChunk {
	ColumnChunk getColumnChunk(int index);

	ColumnType<?, ?>[] getTypes();
}
