package org.knime.core.data.array;

import org.knime.core.data.ChunkFactory;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.column.struct.StructChunk;
import org.knime.core.data.column.struct.StructType;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.type.DoubleChunk;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringChunk;
import org.knime.core.data.type.StringType;

public class ArrayRowBatchFactory implements RowBatchFactory {

	private ChunkFactory<? extends ColumnChunk>[] m_factories;
	private ColumnType<?, ?>[] m_types;
	private int m_chunkSize;

	@SuppressWarnings("unchecked")
	public ArrayRowBatchFactory(ColumnType<?, ?>[] types, int chunkSize) {
		m_types = types;
		m_factories = new ChunkFactory[types.length];
		m_chunkSize = chunkSize;
		for (int i = 0; i < m_types.length; i++) {
			if (m_types[i] instanceof DoubleType) {
				m_factories[i] = createDoubleFactory();
			} else if (m_types[i] instanceof StringType) {
				m_factories[i] = createStringFactory();
			} else if (m_types[i] instanceof StructType) {
				m_factories[i] = createStructFactory(((StructType) m_types[i]).getColumnTypes(), chunkSize);
			}
		}
	}

	@Override
	public RowBatch create() {
		final ColumnChunk[] columnData = new ColumnChunk[m_types.length];
		for (int i = 0; i < m_factories.length; i++) {
			columnData[i] = m_factories[i].create(m_chunkSize);
		}
		return new DefaultRowBatch(columnData);
	}

	@Override
	public ColumnType<?, ?>[] getColumnTypes() {
		return m_types;
	}

	// TODO move into interface, only thing our backend has to implement
	// TODO check the above for nested
	private ChunkFactory<StringChunk> createStringFactory() {
		return (c) -> new StringArrayChunk(c);
	}

	private ChunkFactory<DoubleChunk> createDoubleFactory() {
		return (c) -> new DoubleArrayChunk(c);
	}

	private ChunkFactory<StructChunk> createStructFactory(ColumnType<?, ?>[] childrenTypes, int chunkSize) {
		throw new UnsupportedOperationException("nyi");
	}

}
