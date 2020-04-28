package org.knime.core.data.type;

import org.knime.core.data.domain.Domain;

public class DoubleDomain implements Domain {

	private long m_numMissing;
	private long m_numNonMissing;
	private double m_minimum;
	private double m_maximum;

	public DoubleDomain(long numMissing, long numNonMissing, double minimum, double maximum) {
		m_numMissing = numMissing;
		m_numNonMissing = numNonMissing;
		m_minimum = minimum;
		m_maximum = maximum;
	}

	public DoubleDomain() {
		m_numMissing = 0;
		m_numNonMissing = 0;
		m_minimum = Double.MAX_VALUE;
		m_maximum = -Double.MAX_VALUE;
	}

	@Override
	public long getNumMissing() {
		return m_numMissing;
	}

	@Override
	public long getNumNonMissing() {
		return m_numNonMissing;
	}

	public double getMinimum() {
		return m_minimum;
	}

	public double getMaximum() {
		return m_maximum;
	}

}
