package org.knime.core.data.array;

import org.knime.core.data.type.DoubleChunk;

class DoubleArrayData extends AbstractNativeArray<double[]> implements DoubleChunk {

	// Read case
	public DoubleArrayData(int capacity) {
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
