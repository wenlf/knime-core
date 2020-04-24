package org.knime.core.data.type;

import java.sql.Time;
import java.util.Date;

import org.knime.core.data.column.struct.StructChunk;
import org.knime.core.data.column.struct.StructChunkAccess;
import org.knime.core.data.value.DateTimeReadValue;
import org.knime.core.data.value.DateTimeWriteValue;
import org.knime.core.data.value.ReadValue;
import org.knime.core.data.value.WriteValue;

// TODO Abstract for all logical types.
class DateTimeAccess implements StructChunkAccess<StructChunk> {

	private int m_index = -1;

	private DoubleChunk m_dateData;
	private DoubleChunk m_timeData;

	private StructChunk m_data;

	@Override
	public void fwd() {
		m_index++;
	}

	@Override
	public void reset() {
		m_index = -1;
	}

	@Override
	public void update(final StructChunk data) {
		m_data = data;
		m_dateData = (DoubleChunk) data.getColumnChunk(0);
		m_timeData = (DoubleChunk) data.getColumnChunk(1);
		m_index = -1;
	}

	@Override
	public ReadValue read() {
		return new DefaultDateTimeReadValue();
	}

	@Override
	public WriteValue write() {
		return new DefaultDateTimeWriteValue();
	}

	class DefaultDateTimeReadValue implements DateTimeReadValue {

		@Override
		public boolean isMissing() {
			return m_data.isMissing(m_index);
		}

		@Override
		public Date getDate() {
			return new Date((long) m_dateData.getDouble(m_index));
		}

		@Override
		public Time getTime() {
			return new Time((long) m_timeData.getDouble(m_index));
		}
	}

	class DefaultDateTimeWriteValue implements DateTimeWriteValue {

		@Override
		public void setMissing() {
			m_data.setMissing(m_index);
		}

		@Override
		public void write(Date date) {
			m_dateData.setDouble(m_index, date.getDate());
		}

		@Override
		public void write(Time time) {
			m_dateData.setDouble(m_index, time.getDate());
		}
	}
}
