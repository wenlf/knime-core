package org.knime.core.data.column.struct;

public abstract class AbstractStructChunkAccess implements StructChunkAccess<StructChunk> {

	protected int m_index = -1;
	protected StructChunk m_data;

	protected AbstractStructChunkAccess() {
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
	public void load(StructChunk data) {
		updateInternal(data);
		m_data = data;
		m_index = -1;
	}

	protected abstract void updateInternal(StructChunk data);
}
