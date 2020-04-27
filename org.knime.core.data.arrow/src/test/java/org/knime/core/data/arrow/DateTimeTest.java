package org.knime.core.data.arrow;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.knime.core.data.type.DateTimeAccess;
import org.knime.core.data.value.DateTimeReadValue;
import org.knime.core.data.value.DateTimeWriteValue;

public class DateTimeTest extends AbstractArrowTest {

	@Test
	public void dateTimeData() {
		try (final BufferAllocator alloc = newAllocator()) {

			StructVector vector = StructVector.empty("Struct", alloc);
			Float8VectorChunk dateChunk = new Float8VectorChunk(vector.addOrGet("Date",
					new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
					Float8Vector.class));
			Float8VectorChunk timeChunk = new Float8VectorChunk(vector.addOrGet("Time",
					new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
					Float8Vector.class));
			StructVectorChunk structChunk = new StructVectorChunk(vector, dateChunk, timeChunk);
			structChunk.allocateNew(3);

			DateTimeAccess access = new DateTimeAccess();
			access.load(structChunk);
			access.reset();

			int tmp = 0;

			DateTimeWriteValue write = access;
			for (int i = 0; i < structChunk.getMaxCapacity(); i++) {
				access.fwd();
				write.setDateTime(i + 5, i + 3);
				tmp++;
			}

			assertEquals(3, tmp);
			structChunk.setNumValues(3);
			access.reset();
			tmp = 0;

			DateTimeReadValue read = access;
			for (int i = 0; i < structChunk.getNumValues(); i++) {
				access.fwd();
				assertEquals(i + 5, read.getDate(), 0.0000000000001);
				assertEquals(i + 3, read.getTime(), 0.0000000000001);
				tmp++;
			}

			assertEquals(3, tmp);
			structChunk.release();
		}
	}
}
