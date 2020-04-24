package org.knime.core.data.type;

import org.knime.core.data.column.ColumnChunk;

// NB: Marker interface for Numeric data
public interface NumericChunk extends ColumnChunk {

	double getDouble(int index);

	void setDouble(int index, double val);
}