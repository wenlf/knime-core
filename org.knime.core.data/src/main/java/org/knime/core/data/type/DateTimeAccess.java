package org.knime.core.data.type;

import java.sql.Time;
import java.util.Date;

import org.knime.core.data.column.struct.AbstractStructChunkAccess;
import org.knime.core.data.column.struct.StructChunk;
import org.knime.core.data.value.DateTimeReadValue;
import org.knime.core.data.value.DateTimeWriteValue;
import org.knime.core.data.value.ReadValue;
import org.knime.core.data.value.WriteValue;

class DateTimeAccess extends AbstractStructChunkAccess {

	private DoubleChunk m_dateChunk;
	private DoubleChunk m_timeChunk;

	@Override
	public ReadValue read() {
		return new DefaultDateTimeReadValue();
	}

	@Override
	public WriteValue write() {
		return new DefaultDateTimeWriteValue();
	}

	@Override
	protected void updateInternal(StructChunk data) {
		m_dateChunk = (DoubleChunk) data.getColumnChunk(0);
		m_timeChunk = (DoubleChunk) data.getColumnChunk(1);
	}

	/*
	 * TODO obviously this implementation sucks, just a POC for StructTypes
	 */

	class DefaultDateTimeReadValue implements DateTimeReadValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public Date getDate() {
			return new Date((long) m_dateChunk.getDouble(m_index));
		}

		@Override
		public Time getTime() {
			return new Time((long) m_timeChunk.getDouble(m_index));
		}
	}

	class DefaultDateTimeWriteValue implements DateTimeWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void write(Date date) {
			m_dateChunk.setDouble(m_index, date.getDate());
		}

		@Override
		public void write(Time time) {
			m_timeChunk.setDouble(m_index, time.getTime());
		}
	}

}
