package org.knime.core.data.array;

import org.knime.core.data.ChunkFactory;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.AbstractRowBatchFactory;
import org.knime.core.data.type.DoubleChunk;
import org.knime.core.data.type.StringChunk;

public class ArrayFormatRecordFactory extends AbstractRowBatchFactory {

	public ArrayFormatRecordFactory(ColumnType<?, ?>[] types, int chunkSize) {
		super(types, chunkSize);
	}

	@Override
	public ChunkFactory<StringChunk> createStringDataFactory(int chunkSize) {
		return () -> new StringArrayData(chunkSize);
	}

	@Override
	public ChunkFactory<DoubleChunk> createDoubleDataFactory(int chunkSize) {
		return () -> new DoubleArrayData(chunkSize);
	}

	@Override
	public ChunkFactory<? extends ColumnChunk> createLogicalTypeDataFactory(ColumnType<?, ?>[] childrenTypes,
			int chunkSize) {
		// TODO Auto-generated method stub
		return null;
	}

}
