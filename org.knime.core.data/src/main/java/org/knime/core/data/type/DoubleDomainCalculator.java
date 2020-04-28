package org.knime.core.data.type;

import org.knime.core.data.domain.DomainCalculator;

public class DoubleDomainCalculator implements DomainCalculator<DoubleChunk, DoubleDomain> {
	@Override
	public DoubleDomain apply(DoubleChunk t) {
		long numMissing = 0;
		double min = Double.MAX_VALUE;
		double max = -Double.MAX_VALUE;
		for (int i = 0; i < t.getNumValues(); i++) {
			if (t.isMissing(i)) {
				numMissing++;
			} else {
				// TODO Infinity / NaN handling should be consistent to KNIME.
				double curr = t.getDouble(i);
				if (curr < min) {
					min = curr;
				} else if (curr > max) {
					max = curr;
				}
			}
		}
		return new DoubleDomain(numMissing, t.getNumValues() - numMissing, min, max);
	}

	@Override
	public DoubleDomain merge(DoubleDomain one, DoubleDomain other) {
		// TODO ugly.
		final long numMissing = one.getNumMissing() + other.getNumMissing();
		final long numNonMissing = one.getNumNonMissing() + other.getNumNonMissing();
		final double minimum = Math.min(one.getMinimum(), other.getMinimum());
		final double maximum = Math.max(one.getMaximum(), other.getMaximum());
		return new DoubleDomain(numMissing, numNonMissing, minimum, maximum);
	}

	@Override
	public DoubleDomain createEmpty() {
		return new DoubleDomain();
	}
}