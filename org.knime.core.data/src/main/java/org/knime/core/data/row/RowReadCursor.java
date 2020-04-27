package org.knime.core.data.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.value.ReadValue;

//TODO similar logic required later for columnar access...
//TODO interface
public final class RowReadCursor implements AutoCloseable {

	private final RowBatchReader m_reader;
	private final RowBatchAccess m_access;
	private final int m_numChunks;

	private int m_dataIndex = 0;
	private int m_currentDataMaxIndex;
	private int m_index = -1;

	private RowBatch m_currentData;

	public RowReadCursor(final RowBatchReader reader, final RowBatchAccess access) {
		m_reader = reader;
		m_access = access;
		m_numChunks = m_reader.getNumChunks();

		switchToNextData();
	}

	public void fwd() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
			m_index = 0;
		}
		m_access.fwd();
	}

	// user can keep access while iterating over table
	public <R extends ReadValue> R get(int index) {
		return m_access.getReadValue(index);
	}

	// User can keep list while iterating over table
	// TODO performance
	// TODO share code with RangeReadCursor
	public <R extends ReadValue> ReadValueRange<R> getRange(int startIndex, int length) {
		// TODO check bounds
		return new ReadValueRange<R>() {
			final List<R> m_accesses = new ArrayList<R>();
			{
				for (int i = startIndex; i < length; i++) {
					m_accesses.add(m_access.getReadValue(i));
				}
			}

			// zero based index accesses
			@Override
			public R getReadValue(int index) {
				// TODO performance
				return m_accesses.get(index);
			}
		};
	}

	public boolean canFwd() {
		return m_index < m_currentDataMaxIndex || m_dataIndex < m_numChunks;
	}

	private void switchToNextData() {
		try {
			releaseCurrentData();
			m_currentData = m_reader.read(m_dataIndex++);
			m_access.load(m_currentData);
			m_currentDataMaxIndex = m_currentData.getNumValues() - 1;
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void releaseCurrentData() {
		if (m_currentData != null) {
			m_currentData.release();
		}
	}

	@Override
	public void close() throws Exception {
		releaseCurrentData();
	}
}