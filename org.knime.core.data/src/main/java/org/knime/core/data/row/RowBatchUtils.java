package org.knime.core.data.row;

import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnChunkAccess;
import org.knime.core.data.column.ColumnType;

public final class RowBatchUtils {

	private RowBatchUtils() {
	}

	// TODO cast OK?
	@SuppressWarnings("unchecked")
	public static RowBatchAccess createAccess(final ColumnType<?, ?>[] m_types) {
		final ColumnChunkAccess<ColumnChunk>[] accesses = new ColumnChunkAccess[m_types.length];
		for (int i = 0; i < accesses.length; i++) {
			accesses[i] = (ColumnChunkAccess<ColumnChunk>) m_types[i].createAccess();
		}
		return new RowBatchAccess(accesses);
	}
}
