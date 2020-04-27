package org.knime.core.data.row;

import org.knime.core.data.value.WriteValue;

public interface WriteValueRange<W extends WriteValue> {
	/**
	 * TODO current behaviour: dedicated value object per index. Do we want to keep
	 * that behaviour? For performance reasons only using a single object is
	 * benefitial (less objects).
	 * 
	 * @param index
	 * @return
	 */
	W getWriteValue(int index);
}
