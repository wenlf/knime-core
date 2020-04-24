package org.knime.core.data.value;

import java.sql.Time;
import java.util.Date;

public interface DateTimeWriteValue extends WriteValue {
	void write(Date date);

	void write(Time time);
}
