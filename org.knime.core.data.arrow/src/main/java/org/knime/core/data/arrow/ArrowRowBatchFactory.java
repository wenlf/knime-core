package org.knime.core.data.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.data.ChunkFactory;
import org.knime.core.data.arrow.type.Float8VectorChunk;
import org.knime.core.data.arrow.type.StructVectorChunk;
import org.knime.core.data.arrow.type.VarCharVectorChunk;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.column.struct.StructType;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringType;

public class ArrowRowBatchFactory implements RowBatchFactory {

	private final BufferAllocator m_allocator;
	private final ColumnType<?, ?>[] m_types;
	private int m_chunkSize;
	private final ChunkFactory<FieldVectorChunk<?>>[] m_factories;

	// TODO move chunk size into create for dynamic chunk sizes
	public ArrowRowBatchFactory(ColumnType<?, ?>[] types, BufferAllocator allocator, int chunkSize) {
		m_types = types;
		m_chunkSize = chunkSize;
		m_allocator = allocator;
		m_factories = createColumns(false, m_types);
	}

	@Override
	public RowBatch create() {
		final ColumnChunk[] chunks = new ColumnChunk[m_factories.length];
		for (int i = 0; i < m_factories.length; i++) {
			chunks[i] = m_factories[i].create(m_chunkSize);
		}
		return new DefaultRowBatch(m_chunkSize, chunks);
	}

	@Override
	public ColumnType<?, ?>[] getColumnTypes() {
		return m_types;
	}

	// TODO vector naming (e.g. with TableSchema class...?)

	// creates nested structs recursively.
	// TODO outside facing
	private ChunkFactory<FieldVectorChunk<?>>[] createColumns(boolean hasParent, final ColumnType<?, ?>... types) {
		@SuppressWarnings("unchecked")
		final ChunkFactory<FieldVectorChunk<?>>[] factories = new ChunkFactory[m_types.length];
		for (int i = 0; i < factories.length; i++) {
			if (types[i] instanceof DoubleType) {
				factories[i] = (c) -> allocateNew(new Float8VectorChunk(m_allocator), hasParent, c);
			} else if (m_types[i] instanceof StringType) {
				factories[i] = (c) -> allocateNew(new VarCharVectorChunk(m_allocator), hasParent, c);
			} else if (m_types[i] instanceof StructType) {
				final ChunkFactory<FieldVectorChunk<?>>[] childFactories = createColumns(true,
						((StructType) m_types[i]).getColumnTypes());
				factories[i] = (c) -> {
					final ArrowStructVector structVector = new ArrowStructVector("StructVector", m_allocator);
					final FieldVectorChunk<?>[] childColumns = new FieldVectorChunk[childFactories.length];
					for (int j = 0; j < childFactories.length; j++) {
						childColumns[j] = childFactories[j].create(c);
						structVector.putVectorInternal("Child", childColumns[j].get());
					}
					return allocateNew(new StructVectorChunk(structVector, childColumns), hasParent, c);
				};
			}
		}
		return factories;

	}

	private <C extends FieldVectorChunk<?>> C allocateNew(C chunk, boolean hasParent, int chunkSize) {
		if (!hasParent) {
			chunk.allocateNew(chunkSize);
		}
		return chunk;
	}

	// NB: just to access 'putVector' directly. Performance!
	private class ArrowStructVector extends StructVector {
		public ArrowStructVector(String name, BufferAllocator alloc) {
			super("Struct", alloc, FieldType.nullable(Struct.INSTANCE), null);
		}

		void putVectorInternal(String name, FieldVector vector) {
			putVector(name, vector);
		}
	}

	@Override
	public void setChunkSize(int chunkSize) {
		m_chunkSize = chunkSize;
	}
}
