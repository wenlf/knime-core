package org.knime.core.data;

public interface Referenced {

	/**
	 * Release reference
	 */
	void release();

	/**
	 * Retain reference
	 */
	void retain();

}
