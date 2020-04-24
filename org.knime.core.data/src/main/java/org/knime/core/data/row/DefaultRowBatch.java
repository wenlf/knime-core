package org.knime.core.data.row;

import org.knime.core.data.column.ColumnChunk;

public class DefaultRowBatch implements RowBatch {

	protected ColumnChunk[] m_data;

	private int m_numValues;

	public DefaultRowBatch(ColumnChunk... data) {
		m_numValues = data[0].getNumValues();
		m_data = data;
	}

	// TODO share interface with nested types
	@Override
	public ColumnChunk[] getRecordData() {
		return m_data;
	}

	@Override
	public int getMaxCapacity() {
		return m_data[0].getMaxCapacity();
	}

	@Override
	public void setNumValues(int numValues) {
		for (final ColumnChunk data : m_data) {
			data.setNumValues(numValues);
		}
	}

	@Override
	public int getNumValues() {
		return m_numValues;
	}

	@Override
	public void release() {
		for (int i = 0; i < m_data.length; i++) {
			m_data[i].release();
		}
	}

	@Override
	public void retain() {
		for (int i = 0; i < m_data.length; i++) {
			m_data[i].retain();
		}
	}

}
