package org.knime.core.data.domain;

public interface Domain {

	long getNumMissing();

	long getNumNonMissing();

	default long getNumValues() {
		return getNumMissing() + getNumNonMissing();
	}

}
