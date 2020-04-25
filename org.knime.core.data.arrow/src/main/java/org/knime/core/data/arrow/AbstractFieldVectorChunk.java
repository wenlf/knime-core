package org.knime.core.data.arrow;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

abstract class AbstractFieldVectorChunk<F extends FieldVector> implements FieldVectorChunk<F> {

	protected final F m_vector;

	private final AtomicInteger m_refCounter = new AtomicInteger(1);

	AbstractFieldVectorChunk(BufferAllocator allocator) {
		m_vector = create(allocator);
	}

	protected abstract F create(BufferAllocator allocator);

	AbstractFieldVectorChunk(F vector) {
		m_vector = vector;
	}

	@Override
	public F get() {
		return m_vector;
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

	// TODO thread safety for ref-counting
	// TODO extract to super class / interface
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

}
