package org.knime.core.data.type;

import org.knime.core.data.column.AbstractColumnChunkAccess;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;

public class DoubleAccess extends AbstractColumnChunkAccess<DoubleChunk> implements DoubleReadValue, DoubleWriteValue {

	@Override
	public void setMissing() {
		m_data.setMissing(m_index);
	}

	@Override
	public void setDouble(double value) {
		m_data.setDouble(m_index, value);
	}

	@Override
	public boolean isMissing() {
		return m_data.isMissing(m_index);
	}

	@Override
	public double getDouble() {
		return m_data.getDouble(m_index);
	}
}
