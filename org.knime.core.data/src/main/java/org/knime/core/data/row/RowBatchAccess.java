package org.knime.core.data.row;

import org.knime.core.data.Access;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnChunkAccess;
import org.knime.core.data.value.ReadValue;
import org.knime.core.data.value.WriteValue;

// TODO interface?
public class RowBatchAccess implements Access<RowBatch> {

	private ColumnChunkAccess<ColumnChunk>[] m_accesses;

	RowBatchAccess(ColumnChunkAccess<ColumnChunk>[] accesses) {
		m_accesses = accesses;
	}

	public <R extends ReadValue> R getReadValue(int index) {
		// TODO here we can throw and handle a class cast exception
		@SuppressWarnings("unchecked")
		final R read = (R) m_accesses[index];
		return read;
	}

	public <W extends WriteValue> W getWriteValue(int index) {
		// TODO here we can throw and handle a class cast exception
		@SuppressWarnings("unchecked")
		final W write = (W) m_accesses[index];
		return write;
	}

	@Override
	public void fwd() {
		// TODO could we optimize this method call away?
		for (int i = 0; i < m_accesses.length; i++) {
			m_accesses[i].fwd();
		}
	}

	// TODO even if only called on 'chunk-switch': Can we get rid of the
	// 'record.getData()' method call
	@Override
	public void load(final RowBatch record) {
		final ColumnChunk[] data = record.getRecordData();
		for (int i = 0; i < data.length; i++) {
			m_accesses[i].load(data[i]);
			m_accesses[i].reset();
		}
	}

	@Override
	public void reset() {
		for (int i = 0; i < m_accesses.length; i++) {
			m_accesses[i].reset();
		}
	}
}