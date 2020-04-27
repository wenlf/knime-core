package org.knime.core.data.type;

import org.knime.core.data.column.struct.AbstractStructChunkAccess;
import org.knime.core.data.column.struct.StructChunk;
import org.knime.core.data.value.DateTimeReadValue;
import org.knime.core.data.value.DateTimeWriteValue;

/*
 * TODO obviously this implementation sucks, just a POC for StructTypes
 */
public class DateTimeAccess extends AbstractStructChunkAccess implements DateTimeReadValue, DateTimeWriteValue {

	private DoubleChunk m_dateChunk;
	private DoubleChunk m_timeChunk;

	@Override
	protected void updateInternal(StructChunk data) {
		m_dateChunk = (DoubleChunk) data.getColumnChunk(0);
		m_timeChunk = (DoubleChunk) data.getColumnChunk(1);
	}

	@Override
	public void setMissing() {
		m_data.setMissing(m_index);
	}

	@Override
	public void setDateTime(double date, double time) {
		m_dateChunk.setDouble(m_index, date);
		m_timeChunk.setDouble(m_index, time);
	}

	@Override
	public boolean isMissing() {
		return m_data.isMissing(m_index);
	}

	@Override
	public double getDate() {
		return m_dateChunk.getDouble(m_index);
	}

	@Override
	public double getTime() {
		return m_timeChunk.getDouble(m_index);
	}

}
