package org.knime.core.data.type;

import org.knime.core.data.column.AbstractColumnChunkAccess;
import org.knime.core.data.value.StringReadValue;
import org.knime.core.data.value.StringWriteValue;

//TODO we may want to implement dictionary encoding on this level?!
public class StringAccess extends AbstractColumnChunkAccess<StringChunk> implements StringReadValue, StringWriteValue {

	@Override
	public void setMissing() {
		m_data.setMissing(m_index);
	}

	@Override
	public void setStringValue(String value) {
		m_data.setString(m_index, value);
	}

	@Override
	public boolean isMissing() {
		return m_data.isMissing(m_index);
	}

	@Override
	public String getStringValue() {
		return m_data.getString(m_index);
	}
}
