package org.knime.core.data.column.struct;

import org.knime.core.data.column.ColumnType;

/**
 * A logical type representing a composition of multiple physical types, e.g.
 * DateTime
 */
public interface StructType extends ColumnType<StructChunk, StructChunkAccess<StructChunk>> {
	ColumnType<?, ?>[] getChildrenTypes();
}
