package org.knime.core.data.domain;

import org.knime.core.data.type.NumericChunk;
import org.knime.core.data.value.NumericReadValue;

public interface MutableNumericDomain<D extends NumericChunk, A extends NumericReadValue>
		extends NumericDomain<A>, MutableDomain<D> {
}
