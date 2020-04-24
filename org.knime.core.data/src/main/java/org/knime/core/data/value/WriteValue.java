
package org.knime.core.data.value;

/**
 * Base interface for proxies through which data values are written.
 */
public interface WriteValue {

	// TODO what is the default?
	void setMissing();
}
