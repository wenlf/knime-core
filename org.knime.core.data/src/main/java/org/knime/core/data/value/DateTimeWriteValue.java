package org.knime.core.data.value;


public interface DateTimeWriteValue extends WriteValue {
	void setDate(double date);
	void setTime(double date);
}
