package org.knime.core.data.arrow;

import java.util.function.Supplier;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.column.ColumnChunk;

public interface FieldVectorData<F extends FieldVector> extends Supplier<F>, ColumnChunk {
	// NB: Marker interface
}
