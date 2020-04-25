package org.knime.core.data.arrow;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.complex.StructVector;
import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.struct.StructChunk;

public class StructVectorChunk implements StructChunk, FieldVectorChunk<StructVector> {

	private final FieldVectorChunk<?>[] m_columns;
	private final StructVector m_vector;
	private final AtomicInteger m_refCounter = new AtomicInteger(1);

	StructVectorChunk(StructVector vector, FieldVectorChunk<?>[] columns) {
		m_columns = columns;
		m_vector = vector;
	}

	@Override
	public void setMissing(int index) {
		m_vector.setNull(index);
	}

	@Override
	public boolean isMissing(int index) {
		return m_vector.isNull(index);
	}

	@Override
	public int getMaxCapacity() {
		return m_vector.getValueCapacity();
	}

	@Override
	public int getNumValues() {
		return m_vector.getValueCount();
	}

	@Override
	public void setNumValues(int numValues) {
		m_vector.setValueCount(numValues);
	}

	@Override
	public void release() {
		if (m_refCounter.decrementAndGet() == 0) {
			m_vector.close();
		}
	}

	@Override
	public void retain() {
		m_refCounter.getAndIncrement();
	}

	@Override
	public ColumnChunk getColumnChunk(int index) {
		return m_columns[index];
	}

	@Override
	public StructVector get() {
		return m_vector;
	}

	@Override
	public void allocateNew(int chunkSize) {
		m_vector.setInitialCapacity(chunkSize);
	}

}
