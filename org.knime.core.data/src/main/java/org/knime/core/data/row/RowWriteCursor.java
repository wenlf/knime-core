package org.knime.core.data.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.value.WriteValue;

//TODO similar logic required later for columnar access...
//TODO interface cursor, interface 'get', 'getRange' (late 'getSelection').
public class RowWriteCursor implements AutoCloseable {

	private final RowBatchWriter m_writer;
	private final RowBatchFactory m_factory;
	private final RowBatchAccess m_access;

	private RowBatch m_currentData;
	private long m_currentDataMaxIndex;
	private int m_index = -1;

	public RowWriteCursor(final RowBatchFactory factory, final RowBatchWriter writer, final RowBatchAccess access) {
		m_writer = writer;
		m_factory = factory;
		m_access = access;

		switchToNextData();
	}

	public void fwd() {
		if (++m_index > m_currentDataMaxIndex) {
			switchToNextData();
			m_index = 0;
		}
		m_access.fwd();
	}

	public <W extends WriteValue> W get(int index) {
		return m_access.getWriteValue(index);
	}

	// User can keep list while iterating over table
	// TODO share code with RangeReadCursor
	// TODO performance
	public <W extends WriteValue> WriteValueRange<W> getRange(int startIndex, int length) {
		// TODO check bounds
		return new WriteValueRange<W>() {
			private final List<W> m_accesses = new ArrayList<W>();
			{
				for (int i = startIndex; i < length; i++) {
					m_accesses.add(m_access.getReadValue(i));
				}
			}

			// zero based index accesses
			@Override
			public W getWriteValue(int index) {
				// TODO performance
				return m_accesses.get(index);
			}
		};
	}

	@Override
	public void close() throws Exception {
		releaseCurrentData(m_index + 1);
		m_writer.close();
	}

	private void switchToNextData() {
		releaseCurrentData(m_index);
		m_currentData = m_factory.create();
		m_access.load(m_currentData);
		m_currentDataMaxIndex = m_currentData.getMaxCapacity() - 1;
	}

	private void releaseCurrentData(int numValues) {
		if (m_currentData != null) {
			m_currentData.setNumValues(numValues);
			m_writer.write(m_currentData);
			m_currentData.release();
		}
	}
}