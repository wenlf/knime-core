package org.knime.core.data.array;

import org.knime.core.data.type.DoubleChunk;

class DoubleArrayChunk extends AbstractNativeArrayChunk<double[]> implements DoubleChunk {

	// Read case
	public DoubleArrayChunk(int capacity) {
		super(new double[capacity], capacity);
	}

	@Override
	public double getDouble(int index) {
		return m_array[index];
	}

	@Override
	public void setDouble(int index, double val) {
		m_array[index] = val;
	}
}
