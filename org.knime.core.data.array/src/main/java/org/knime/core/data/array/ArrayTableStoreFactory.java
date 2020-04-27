package org.knime.core.data.array;

import java.io.File;

import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowBatchWriter;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreConfig;
import org.knime.core.data.table.store.TableStoreFactory;

public class ArrayTableStoreFactory implements TableStoreFactory {

	private int m_chunkSize;

	public ArrayTableStoreFactory(int chunkSize) {
		m_chunkSize = chunkSize;
	}

	@Override
	public void close() throws Exception {
		// NB: nothing for now
	}

	@Override
	public TableStore create(ColumnType<?, ?>[] types, File file, TableStoreConfig hints) {
		return new TableStore() {

			@Override
			public RowBatchFactory createFactory() {
				// TODO change interface... see 'AbstractRecordFactory'
				return new ArrayRowBatchFactory(types, m_chunkSize);
			}

			@Override
			public void close() throws Exception {
			}

			@Override
			public RowBatchReader createReader(RowBatchReaderConfig config) {
				return new RowBatchReader() {

					@Override
					public void close() throws Exception {

					}

					@Override
					public RowBatch read(int chunkIndex) {
						return null;
					}

					@Override
					public int getNumChunks() {
						return 0;
					}
				};
			}

			@Override
			public RowBatchWriter getWriter() {
				return new RowBatchWriter() {

					@Override
					public void close() throws Exception {

					}

					@Override
					public void write(RowBatch record) {

					}
				};
			}

			@Override
			public ColumnType<?, ?>[] getColumnTypes() {
				return types;
			}
		};
	}
}
