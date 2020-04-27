package org.knime.core.data;

// TODO type on ReadAcces / WriteAccess?
public interface Access<O> {
	/**
	 * Fwd the internal cursor
	 */
	void fwd();

	/**
	 * resets internal cursor to -1
	 */
	void reset();

	/**
	 * Updates underlying data.
	 * 
	 * @param obj
	 */
	void load(O obj);
}
