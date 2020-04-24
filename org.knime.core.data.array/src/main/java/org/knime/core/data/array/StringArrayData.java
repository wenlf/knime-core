package org.knime.core.data.array;

import org.knime.core.data.type.StringChunk;

// TODO we may want to store sparse arrays etc differently.
class StringArrayData extends AbstractArray<String[]> implements StringChunk {

	// Read case
	public StringArrayData(int capacity) {
		super(new String[capacity], capacity);
	}

	@Override
	public String getString(int index) {
		return m_array[index];
	}

	@Override
	public void setString(int index, String val) {
		m_array[index] = val;
	}

	@Override
	public void setMissing(int index) {
		m_array[index] = null;
	}

	@Override
	public boolean isMissing(int index) {
		return m_array[index] == null;
	}
}
