package org.knime.core.data.column;

// looks like an enum, but isn't! We want to support nesting, complex types etc.
// TODO Allow to create empty domains for types. Interface. Not every type has a domain currently.
public interface ColumnType<D extends ColumnChunk, A extends ColumnChunkAccess<D>> {
	A createAccess();
}
