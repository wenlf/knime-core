package org.knime.core.data.type;

import org.knime.core.data.column.ColumnChunk;

public interface StringChunk extends ColumnChunk {
	String getString(int index);

	void setString(int index, String val);
}