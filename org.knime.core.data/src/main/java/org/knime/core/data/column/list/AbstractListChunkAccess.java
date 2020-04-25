package org.knime.core.data.column.list;

public abstract class AbstractListChunkAccess implements ListChunkAccess<ListChunk> {

	protected int m_index = -1;
	protected ListChunk m_data;

	protected AbstractListChunkAccess() {
	}

	@Override
	public void fwd() {
		m_index++;
	}

	@Override
	public void reset() {
		m_index = -1;
	}

	@Override
	public void update(ListChunk data) {
		m_data = data;
		m_index = -1;
	}
}
