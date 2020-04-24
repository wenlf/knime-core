package org.knime.core.data.column;

public abstract class AbstractColumnChunkAccess<C extends ColumnChunk> implements ColumnChunkAccess<C> {

	protected int m_index;
	protected C m_data;

	@Override
	public void update(C data) {
		m_data = data;
		m_index = -1;
	}

	@Override
	public void fwd() {
		m_index++;
	}

	@Override
	public void reset() {
		m_index = -1;
	}
}
