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

public class ArrayFormat implements TableStoreFactory {

	private int m_chunkSize;

	public ArrayFormat(int chunkSize) {
		m_chunkSize = chunkSize;
	}

	@Override
	public void close() throws Exception {
		// NB: nothing for now
	}

	@Override
	public RowBatchFactory createFactory(ColumnType<?, ?>[] schema) {
		// TODO change interface... see 'AbstractRecordFactory'
		return new ArrayFormatRecordFactory(schema, m_chunkSize);
	}

	@Override
	public TableStore create(ColumnType<?, ?>[] schema, File file, TableStoreConfig hints) {
		return new TableStore() {

			@Override
			public void close() throws Exception {
			}

			@Override
			public RowBatchReader createReader() {
				return new RowBatchReader() {

					@Override
					public void close() throws Exception {

					}

					@Override
					public RowBatch read(int chunkIndex, RowBatchReaderConfig hints) {
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
				return schema;
			}
		};
	}
}
