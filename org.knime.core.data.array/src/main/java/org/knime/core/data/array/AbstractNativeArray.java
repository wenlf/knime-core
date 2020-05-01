package org.knime.core.data.array;

class AbstractNativeArray<A> extends AbstractArray<A> {
	private long[] m_isMissing;

	protected AbstractNativeArray(A array, int capacity) {
		super(array, capacity);
		m_isMissing = new long[((int) capacity / 64) + 1];
	}

	@Override
	public boolean isMissing(int index) {
		// NB: inspired by imglib2
		return 1 == ((m_isMissing[((int) index >>> 6)] >>> ((index & 63))) & 1l);
	}

	@Override
	public void setMissing(int index) {
		// NB: inspired by imglib
		final int i1 = (int) index >>> 6;
		m_isMissing[i1] = m_isMissing[i1] | 1l << (index & 63);
	}
}
