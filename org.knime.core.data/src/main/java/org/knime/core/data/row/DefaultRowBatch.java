package org.knime.core.data.row;

import org.knime.core.data.column.ColumnChunk;

public class DefaultRowBatch implements RowBatch {

	protected ColumnChunk[] m_data;

	private int m_numValues;

	private int m_chunkSize;

	public DefaultRowBatch(ColumnChunk... data) {
		m_numValues = data[0].getNumValues();
		// chunkSize = numValues.
		m_chunkSize = m_numValues;
		m_data = data;
	}

	public DefaultRowBatch(int chunkSize, ColumnChunk... data) {
		this(data);
		// write case.
		m_chunkSize = chunkSize;
	}

	// TODO share interface with nested types
	@Override
	public ColumnChunk[] getRecordData() {
		return m_data;
	}

	@Override
	public int getMaxCapacity() {
		// TODO we have to return the chunk size here. different arrow types will
		// instantiate
		// different max capacities, e.g. based on memory consumption.
		// TODO figure out optimal chunk sizes without wasting memory.
		return m_chunkSize;
	}

	@Override
	public void setNumValues(int numValues) {
		for (final ColumnChunk data : m_data) {
			data.setNumValues(numValues);
		}
		m_numValues = numValues;
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
