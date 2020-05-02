package org.knime.core.data.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.data.arrow.io.FieldVectorReader;
import org.knime.core.data.arrow.io.FieldVectorWriter;
import org.knime.core.data.arrow.type.Float8VectorChunk;
import org.knime.core.data.arrow.type.StructVectorChunk;
import org.knime.core.data.arrow.type.VarCharVectorChunk;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.DefaultRowBatch;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowBatchWriter;
import org.knime.core.data.table.store.TableReadStore;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreConfig;
import org.knime.core.data.table.store.TableStoreFactory;

public class ArrowTableStoreFactory implements TableStoreFactory {

	private final static BufferAllocator ROOT = new RootAllocator();

	public ArrowTableStoreFactory() {
	}

	@Override
	public void close() throws Exception {
		// TODO close? Must be unique over application.
		// ROOT.close();
	}

	@Override
	public TableStore create(ColumnType<?, ?>[] types, File file, TableStoreConfig config) {
		return new ArrowTableStore(file, types, config);
	}

	@Override
	public TableReadStore create(ColumnType<?, ?>[] types, File file, long size) {
		return new ArrowTableReadStore(file, types, ROOT.newChildAllocator("ArrowReadStore", 0, ROOT.getLimit()), size);
	}

	// TODO THINKING: Does the writer determine the read chunk size? also in
	// parquet?
	final class ArrowTableReadStore implements TableReadStore {

		private final ColumnType<?, ?>[] m_types;
		private final File m_file;

		private long m_size;
		private BufferAllocator m_allocator;

		ArrowTableReadStore(File file, ColumnType<?, ?>[] types, final BufferAllocator allocator, long size) {
			m_file = file;
			m_types = types;
			m_allocator = allocator;
			m_size = size;
		}

		ArrowTableReadStore(File file, ColumnType<?, ?>[] types, final BufferAllocator allocator) {
			this(file, types, allocator, 0);
		}

		@Override
		public RowBatchReader createReader(RowBatchReaderConfig config) {
			return new RowBatchReader() {

				// for now one reader per RecordReader
				private final FieldVectorReader m_reader;
				{
					try {
						m_reader = new FieldVectorReader(m_file, m_allocator);
					} catch (IOException e) {
						// TODO
						throw new RuntimeException(e);
					}
				}

				@Override
				public RowBatch read(int chunkIndex) {
					try {
						final FieldVector[] vectors = m_reader.read(chunkIndex);
						final ColumnChunk[] data = new ColumnChunk[vectors.length];
						for (int i = 0; i < data.length; i++) {
							// TODO visitor pattern...
							// TODO synergy with ArrowRowBatchFactory?
							if (vectors[i] instanceof Float8Vector) {
								data[i] = new Float8VectorChunk((Float8Vector) vectors[i]);
							} else if (vectors[i] instanceof StructVector) {
								data[i] = new StructVectorChunk((StructVector) vectors[i]);
							} else if (vectors[i] instanceof VarCharVector) {
								data[i] = new VarCharVectorChunk((VarCharVector) vectors[i]);
							}
						}
						return new DefaultRowBatch(data);
					} catch (IOException e) {
						// TODO
						throw new RuntimeException(e);
					}
				}

				@Override
				public int getNumChunks() {
					return m_reader.getNumChunks();
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
		public long size() {
			return m_size;
		}

		void setSize(long size) {
			m_size = size;
		}

		@Override
		public void close() throws Exception {
			m_allocator.close();
		}

		@Override
		public Class<ArrowTableStoreFactory> getFactory() {
			return ArrowTableStoreFactory.class;
		}
	}

	final class ArrowTableStore implements TableStore {

		private final ColumnType<?, ?>[] m_types;

		// TODO give a more meaningful name than 'ArrowStore' :-)
		private final BufferAllocator m_childAllocator = ROOT.newChildAllocator("ArrowStore", 0, ROOT.getLimit());
		private final File m_file;

		private TableStoreConfig m_config;

		private long m_size;

		private ArrowTableReadStore m_readStore;

		ArrowTableStore(File file, final ColumnType<?, ?>[] types, TableStoreConfig config) {
			m_readStore = new ArrowTableReadStore(file, types, m_childAllocator);
			m_types = types;
			m_file = file;
			m_config = config;
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
							vectorData[i] = ((FieldVectorChunk<?>) recordData[i]).get();
						}
						m_size += record.getNumValues();
						m_readStore.setSize(m_size);
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
		public RowBatchReader createReader(RowBatchReaderConfig config) {
			return m_readStore.createReader(config);
		}

		@Override
		public ColumnType<?, ?>[] getColumnTypes() {
			return m_types;
		}

		@Override
		public void close() throws Exception {
			// TODO close read before write?
			m_childAllocator.close();
			m_readStore.close();
		}

		@Override
		public RowBatchFactory createFactory() {
			// TODO change interface... see 'AbstractRecordFactory'
			// TODO use child allocator per store!!
			return new ArrowRowBatchFactory(m_types, ROOT,
					BaseAllocator.nextPowerOfTwo(m_config.getInitialChunkSize()) - 1);
		}

		@Override
		public long size() {
			return m_size;
		}

		@Override
		public void copyDataTo(File file) throws FileNotFoundException, IOException {
			Files.copy(m_file.toPath(), new FileOutputStream(file));
		}

		@Override
		public Class<ArrowTableStoreFactory> getFactory() {
			return m_readStore.getFactory();
		}
	}
}
