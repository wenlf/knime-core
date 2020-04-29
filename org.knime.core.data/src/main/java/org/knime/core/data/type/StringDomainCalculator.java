package org.knime.core.data.type;

import java.util.Set;
import java.util.TreeSet;

import org.knime.core.data.domain.DomainCalculator;

public class StringDomainCalculator implements DomainCalculator<StringChunk, StringDomain> {

	final int m_threshold;

	public StringDomainCalculator(int threshold) {
		m_threshold = threshold;
	}

	@Override
	public StringDomain apply(StringChunk t) {
		// TODO likely there are more efficient implementations than this...
		Set<String> values = new TreeSet<>();
		long numMissing = 0;
		for (int i = 0; i < t.getNumValues(); i++) {
			if (t.isMissing(i)) {
				numMissing++;
			} else if (values.size() < m_threshold) {
				values.add(t.getString(i));
				if (values.size() > m_threshold) {
					values.clear();
					values = null;
				}
			}
		}

		return new StringDomain(values, numMissing, t.getNumValues() - numMissing);
	}

	@Override
	public StringDomain merge(StringDomain one, StringDomain other) {
		// TODO preserve order of appearance in original table
		Set<String> union;
		if (one.getValues() != null) {
			union = new TreeSet<>(one.getValues());
		} else {
			union = new TreeSet<>();
		}

		if (other.getValues() != null) {
			union.addAll(other.getValues());
		}
		return new StringDomain(union, one.getNumMissing() + other.getNumMissing(),
				one.getNumNonMissing() + other.getNumNonMissing());
	}

	@Override
	public StringDomain createEmpty() {
		return new StringDomain();
	}
}
