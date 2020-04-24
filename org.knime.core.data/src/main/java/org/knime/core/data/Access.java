package org.knime.core.data;

// TODO type on ReadAcces / WriteAccess?
public interface Access<O> {

	/**
	 * Updates underlying data.
	 * 
	 * @param obj
	 */
	void update(O obj);

	/**
	 * Fwd the internal cursor
	 */
	void fwd();

	/**
	 * resets internal cursor to -1
	 */
	void reset();
}
