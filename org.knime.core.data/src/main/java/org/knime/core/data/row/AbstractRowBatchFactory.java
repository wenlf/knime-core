package org.knime.core.data.row;

import org.knime.core.data.ChunkFactory;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.column.struct.StructType;
import org.knime.core.data.type.DoubleChunk;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringChunk;
import org.knime.core.data.type.StringType;

public abstract class AbstractRowBatchFactory implements RowBatchFactory {

	private ChunkFactory<? extends ColumnChunk>[] m_factories;
	private ColumnType<?, ?>[] m_types;

	@SuppressWarnings("unchecked")
	public AbstractRowBatchFactory(ColumnType<?, ?>[] types, int chunkSize) {
		m_types = types;
		m_factories = new ChunkFactory[types.length];
		for (int i = 0; i < m_types.length; i++) {
			if (m_types[i] instanceof DoubleType) {
				m_factories[i] = createDoubleDataFactory(chunkSize);
			} else if (m_types[i] instanceof StringType) {
				m_factories[i] = createStringDataFactory(chunkSize);
			} else if (m_types[i] instanceof StructType) {
				m_factories[i] = createLogicalTypeDataFactory(((StructType) m_types[i]).getChildrenTypes(), chunkSize);
			}
		}
	}

	@Override
	public RowBatch create() {
		final ColumnChunk[] columnData = new ColumnChunk[m_types.length];
		for (int i = 0; i < m_factories.length; i++) {
			columnData[i] = m_factories[i].create();
		}
		return new DefaultRowBatch(columnData);
	}

	@Override
	public ColumnType<?, ?>[] getColumnTypes() {
		return m_types;
	}

	// TODO move into interface, only thing our backend has to implement
	// TODO check the above for nested
	public abstract ChunkFactory<StringChunk> createStringDataFactory(int chunkSize);

	public abstract ChunkFactory<DoubleChunk> createDoubleDataFactory(int chunkSize);

	public abstract ChunkFactory<? extends ColumnChunk> createLogicalTypeDataFactory(ColumnType<?, ?>[] childrenTypes,
			int chunkSize);

}
