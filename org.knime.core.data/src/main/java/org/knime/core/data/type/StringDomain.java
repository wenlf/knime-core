package org.knime.core.data.type;

import java.util.Collections;
import java.util.Set;

import org.knime.core.data.domain.Domain;

public class StringDomain implements Domain {

	private final Set<String> m_values;
	private final long m_numMissing;
	private final long m_numNonMissing;

	public StringDomain(Set<String> values, long numMissing, long numNonMissing) {
		m_values = values;
		m_numMissing = numMissing;
		m_numNonMissing = numNonMissing;
	}

	public StringDomain(long numMissing, long numNonMissing) {
		this(null, numMissing, numNonMissing);
	}

	public StringDomain() {
		this(null, 0, 0);
	}

	@Override
	public long getNumMissing() {
		return m_numMissing;
	}

	@Override
	public long getNumNonMissing() {
		return m_numNonMissing;
	}

	public Set<String> getValues() {
		return m_values != null ? Collections.unmodifiableSet(m_values) : null;
	}

	public boolean hasValues() {
		return m_values != null;
	}

}
