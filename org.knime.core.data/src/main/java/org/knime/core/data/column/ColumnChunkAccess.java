package org.knime.core.data.column;

import org.knime.core.data.Access;
import org.knime.core.data.value.ReadValue;
import org.knime.core.data.value.WriteValue;

public interface ColumnChunkAccess<C extends ColumnChunk> extends Access<C>, ReadValue, WriteValue {

//	// TODO interface?
//	/**
//	 * @return read access on values of the data
//	 */
//	ReadValue read();
//
//	/**
//	 * @return write access on values of data
//	 */
//	WriteValue write();
}
