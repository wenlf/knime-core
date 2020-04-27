package org.knime.core.data.row;

public interface RowBatchReader extends AutoCloseable {

	RowBatch read(int chunkIndex);

	int getNumChunks();
}
