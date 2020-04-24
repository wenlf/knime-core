package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.data.ChunkFactory;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.AbstractRowBatchFactory;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowBatchWriter;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreConfig;
import org.knime.core.data.table.store.TableStoreFactory;
import org.knime.core.data.type.DoubleChunk;
import org.knime.core.data.type.StringChunk;

public class ArrowFormat implements TableStoreFactory {

	private final int m_chunkSize;
	private final BufferAllocator m_root;

	public ArrowFormat(int chunkSize) {
		m_chunkSize = chunkSize;
		m_root = new RootAllocator();
	}

	@Override
	public void close() throws Exception {
		m_root.close();
	}

	@Override
	public TableStore create(ColumnType<?, ?>[] schema, File file, TableStoreConfig hints) {
		return new ArrowRecordStore(file, schema, hints);
	}

	@Override
	public RowBatchFactory createFactory(ColumnType<?, ?>[] schema) {
		// TODO change interface... see 'AbstractRecordFactory'
		// TODO use child allocator per store!!
		return new AbstractRowBatchFactory(schema, m_chunkSize) {

			@Override
			public ChunkFactory<StringChunk> createStringDataFactory(int chunkSize) {
				return () -> new VarCharVectorData(m_root, chunkSize);
			}

			@Override
			public ChunkFactory<DoubleChunk> createDoubleDataFactory(int chunkSize) {
				return () -> new Float8VectorData(m_root, chunkSize);
			}

			@Override
			public ChunkFactory<? extends ColumnChunk> createLogicalTypeDataFactory(ColumnType<?, ?>[] childrenTypes,
					int chunkSize) {
				return null;
			}

		};
	}

	class ArrowRecordStore implements TableStore {

		private final ColumnType<?, ?>[] m_types;

		// TODO give a meaningful name :-)
		private final BufferAllocator m_childAllocator = m_root.newChildAllocator("ArrowStore", 0, m_root.getLimit());
		private final File m_file;

		ArrowRecordStore(File file, final ColumnType<?, ?>[] types, TableStoreConfig hints) {
			m_types = types;
			m_file = file;
		}

		@Override
		public RowBatchWriter getWriter() {
			return new RowBatchWriter() {
				private final FieldVectorWriter m_writer = new FieldVectorWriter(m_file);;

				// TODO type on Record on FieldVector?
				@Override
				public void write(RowBatch record) {
					try {
						final ColumnChunk[] recordData = record.getRecordData();
						final FieldVector[] vectorData = new FieldVector[recordData.length];
						for (int i = 0; i < vectorData.length; i++) {
							vectorData[i] = ((FieldVectorData<?>) recordData[i]).get();
						}
						m_writer.write(vectorData);
					} catch (IOException e) {
						// TODO
						throw new RuntimeException(e);
					}
				}

				@Override
				public void close() throws Exception {
					m_writer.close();
				}
			};
		}

		@Override
		public RowBatchReader createReader() {
			return new RowBatchReader() {

				// for now one reader per RecordReader
				private final FieldVectorReader m_reader;
				{
					try {
						m_reader = new FieldVectorReader(m_file, m_childAllocator);
					} catch (IOException e) {
						// TODO
						throw new RuntimeException(e);
					}
				}

				@Override
				public RowBatch read(int chunkIndex, RowBatchReaderConfig hints) {
					try {
						final FieldVector[] vectors = m_reader.read(chunkIndex);
						final ColumnChunk[] data = new ColumnChunk[vectors.length];
						for (int i = 0; i < data.length; i++) {
							data[i] = new Float8VectorData((Float8Vector) vectors[0]);
						}
						return new DefaultRowBatch(data);
					} catch (IOException e) {
						// TODO
						throw new RuntimeException(e);
					}
				}

				@Override
				public int getNumChunks() {
					return m_reader.size();
				}

				@Override
				public void close() throws Exception {
					m_reader.close();
				}
			};
		}

		@Override
		public ColumnType<?, ?>[] getColumnTypes() {
			return m_types;
		}

		@Override
		public void close() throws Exception {
			m_childAllocator.close();
		}
	}
}
