package org.knime.core.data.domain;

import java.util.function.Function;

import org.knime.core.data.column.ColumnChunk;

public interface DomainCalculator<C extends ColumnChunk, D extends Domain> extends Function<C, D> {

	D createEmpty();

	D merge(D result, D stored);
}
