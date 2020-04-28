package org.knime.core.data.arrow.type;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.type.DoubleChunk;

public class Float8VectorChunk extends AbstractFieldVectorChunk<Float8Vector> implements DoubleChunk {

	public Float8VectorChunk(BufferAllocator allocator) {
		super(allocator);
	}

	public Float8VectorChunk(Float8Vector vector) {
		super(vector);
	}

	@Override
	public void setMissing(int index) {
		m_vector.setNull(index);
	}

	@Override
	protected Float8Vector create(BufferAllocator allocator) {
		final Float8Vector vector = new Float8Vector("Float8Vector", allocator);
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

	@Override
	public void allocateNew(int chunkSize) {
		m_vector.allocateNew(chunkSize);
	}

}
