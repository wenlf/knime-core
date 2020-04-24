package org.knime.core.data;

@FunctionalInterface
public interface ChunkFactory<C extends Chunk> {
	C create();
}
