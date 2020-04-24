package org.knime.core.data.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.type.DoubleChunk;

public class Float8VectorData extends AbstractFieldVectorData<Float8Vector> implements DoubleChunk {

	Float8VectorData(BufferAllocator allocator, int chunkSize) {
		super(allocator, chunkSize);
	}

	Float8VectorData(Float8Vector vector) {
		super(vector);
	}

	@Override
	public void setMissing(int index) {
		m_vector.setNull(index);
	}

	@Override
	protected Float8Vector create(BufferAllocator allocator, int chunkSize) {
		final Float8Vector vector = new Float8Vector("Float8Vector", allocator);
		vector.allocateNew(chunkSize);
		return vector;
	}

	@Override
	public double getDouble(int index) {
		return m_vector.get(index);
	}

	@Override
	public void setDouble(int index, double value) {
		m_vector.set(index, value);
	}

}
