package org.knime.core.data.type;

import org.knime.core.data.column.AbstractColumnChunkAccess;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;

public class DoubleAccess extends AbstractColumnChunkAccess<DoubleChunk> {

	@Override
	public DoubleReadValue read() {
		return new DefaultDoubleReadValue();
	}

	@Override
	public DoubleWriteValue write() {
		return new DefaultDoubleWriteValue();
	}

	// TODO do we find a better approach than this? Is this approach harming
	// performance?
	class DefaultDoubleWriteValue implements DoubleWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void setDouble(double value) {
			m_data.setDouble(m_index, value);
		}

	};

	class DefaultDoubleReadValue implements DoubleReadValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public double getDouble() {
			return m_data.getDouble(m_index);
		}
	};
}
