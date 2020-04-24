package org.knime.core.data.column;

import org.knime.core.data.Chunk;

public interface ColumnChunk extends Chunk {

	/**
	 * @param set value missing at index. Default is false.
	 */
	void setMissing(int index);

	/**
	 * @param index of value
	 * @return true, if value is missing. Default is false.
	 */
	boolean isMissing(int index);
}
