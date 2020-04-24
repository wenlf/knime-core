package org.knime.core.data.row;

public interface RowBatchWriter extends AutoCloseable {
	void write(final RowBatch record);
}
