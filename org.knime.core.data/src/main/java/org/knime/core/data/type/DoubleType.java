package org.knime.core.data.type;

import org.knime.core.data.column.ColumnType;

public class DoubleType implements ColumnType<DoubleChunk, DoubleAccess> {

	public static DoubleType INSTANCE = new DoubleType();

	private DoubleType() {
	}

	@Override
	public DoubleAccess createAccess() {
		return new DoubleAccess();
	}
}
