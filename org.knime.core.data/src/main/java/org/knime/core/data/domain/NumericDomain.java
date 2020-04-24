package org.knime.core.data.domain;

import org.knime.core.data.value.NumericReadValue;

// TODO overdesigned?
public interface NumericDomain<A extends NumericReadValue> extends Domain {
	A getMin();

	A getMax();

	A getSum();
}
