package org.knime.core.data.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.value.ReadValue;
import org.knime.core.data.value.WriteValue;

public class RowUtils {

	// User can keep list while iterating over table
	// TODO performance
	// TODO share code with RangeReadCursor
	public static <R extends ReadValue> ValueRange<R> getRange(final RowReadCursor cursor, int startIndex, int length) {
		// TODO check bounds
		return new ValueRange<R>() {
			final List<R> m_accesses = new ArrayList<R>();
			{
				for (int i = startIndex; i < length; i++) {
					m_accesses.add(cursor.get(i));
				}
			}

			// zero based index accesses
			@Override
			public R get(int index) {
				// TODO performance
				return m_accesses.get(index);
			}
		};
	}

	// User can keep list while iterating over table
	// TODO performance
	// TODO share code with RangeReadCursor
	public static <W extends WriteValue> ValueRange<W> getRange(final RowWriteCursor cursor, int startIndex,
			int length) {
		// TODO check bounds
		return new ValueRange<W>() {
			final List<W> m_accesses = new ArrayList<W>();
			{
				for (int i = startIndex; i < length; i++) {
					m_accesses.add(cursor.get(i));
				}
			}

			// zero based index accesses
			@Override
			public W get(int index) {
				// TODO performance
				return m_accesses.get(index);
			}
		};
	}

	public interface ValueRange<W> {
		/**
		 * TODO current behaviour: dedicated value object per index. Do we want to keep
		 * that behaviour? For performance reasons only using a single object is
		 * benefitial (less objects).
		 * 
		 * @param index
		 * @return
		 */
		W get(int index);
	}

}
