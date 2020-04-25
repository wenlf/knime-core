package org.knime.core.data.arrow;

import java.util.function.Supplier;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.column.ColumnChunk;

public interface FieldVectorChunk<F extends FieldVector> extends Supplier<F>, ColumnChunk {

	void allocateNew(int chunkSize);
	// NB: Marker interface
}
