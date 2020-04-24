package org.knime.core.data.value;

import java.sql.Time;
import java.util.Date;

public interface DateTimeReadValue extends ReadValue {
	Date getDate();

	Time getTime();
}
