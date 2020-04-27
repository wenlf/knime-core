package org.knime.core.data.row;

import org.knime.core.data.value.ReadValue;

public interface ReadValueRange<R extends ReadValue> {
	R getReadValue(int index);
}
