package org.knime.core.data.row;

import org.knime.core.data.value.WriteValue;

public interface WriteValueRange<W extends WriteValue> {
	// returns a new value per index
	W getWriteValue(int index);
}
