
package org.knime.core.data.array;

import org.knime.core.data.Access;

// TODO composition over inheritance? :-(
class AbstractNativeAccess<A extends ArrayChunk> //
		implements Access<A> {

	protected int m_index = -1;
	protected A m_array;

	@Override
	public void update(A data) {
		m_array = data;
		m_index = -1;
	}

	@Override
	public void fwd() {
		m_index++;
	}

	@Override
	public void reset() {
		m_index = -1;
	}

}
