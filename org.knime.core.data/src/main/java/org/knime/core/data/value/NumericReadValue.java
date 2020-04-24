
package org.knime.core.data.value;

/**
 * Base interface for proxies through which data values are read.
 * 
 * TODO do we need that?
 */
public interface NumericReadValue extends ReadValue {
	double getDouble();
}
