package org.knime.core.data.domain;

import org.knime.core.data.Chunk;

public interface MutableDomain<D extends Chunk> extends Domain {
	void update(D data);
}
