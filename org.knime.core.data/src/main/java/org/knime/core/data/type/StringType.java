package org.knime.core.data.type;

import org.knime.core.data.column.ColumnType;

public class StringType implements ColumnType<StringChunk, StringAccess> {

	public static StringType INSTANCE = new StringType();

	private StringType() {
	}

	// Could we also create a 'StringReadValue' or 'ReadWriteValue' here instead of
	// a string access WITHOUT exposing 'update', 'fwd' etc to the end-user?
	@Override
	public StringAccess createAccess() {
		return new StringAccess();
	}

}
