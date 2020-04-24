package org.knime.core.data.type;

import org.knime.core.data.column.ColumnType;
import org.knime.core.data.column.struct.StructType;

class DateTimeType implements StructType {

	@Override
	public DateTimeAccess createAccess() {
		return new DateTimeAccess();
	}

	@Override
	public ColumnType<?, ?>[] getChildrenTypes() {
		return new ColumnType[] { DoubleType.INSTANCE, DoubleType.INSTANCE };
	}
}