package org.knime.core.data.arrow.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;

import io.netty.buffer.ArrowBuf;

public class FieldVectorWriter implements AutoCloseable {

	private final File m_file;
	private ArrowFileWriter m_writer;
	private VectorLoader m_vectorLoader;
	private VectorSchemaRoot m_root;

	public FieldVectorWriter(final File file) {
		m_file = file;
	}

	@SuppressWarnings("resource")
	public void write(FieldVector[] vecs) throws IOException {

		if (m_writer == null) {
			final ArrayList<Field> fields = new ArrayList<>(vecs.length);
			for (final FieldVector v : vecs) {
				fields.add(v.getField());
			}
			m_root = new VectorSchemaRoot(fields, Arrays.asList(vecs));
			m_vectorLoader = new VectorLoader(m_root);
			m_writer = new ArrowFileWriter(m_root, null, new RandomAccessFile(m_file, "rw").getChannel());
		}
		

		// TODO there must be a better way?!
		final List<ArrowFieldNode> nodes = new ArrayList<>();
		final List<ArrowBuf> buffers = new ArrayList<>();
		for (FieldVector vec : vecs) {
			appendNodes(vec, nodes, buffers);
		}

		// Auto-closing makes sure that ArrowRecordBatch actually releases the buffers
		// again
		try (final ArrowRecordBatch batch = new ArrowRecordBatch((int) vecs[0].getValueCount(), nodes, buffers)) {
			m_vectorLoader.load(batch);
			m_writer.writeBatch();
		}
	}

	@Override
	public void close() throws Exception {
		if (m_writer != null) {
			m_writer.close();
			m_root.close();
		}
	}

	// TODO: Copied from org.apache.arrow.vector.VectorUnloader. Is there a better
	// way to do all of this (including writing vectors in general)?
	private void appendNodes(final FieldVector vector, final List<ArrowFieldNode> nodes, final List<ArrowBuf> buffers) {
		nodes.add(new ArrowFieldNode(vector.getValueCount(), vector.getNullCount()));
		final List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
		final int expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
		if (fieldBuffers.size() != expectedBufferCount) {
			throw new IllegalArgumentException(
					String.format("wrong number of buffers for field %s in vector %s. found: %s", vector.getField(),
							vector.getClass().getSimpleName(), fieldBuffers));
		}
		buffers.addAll(fieldBuffers);
		for (final FieldVector child : vector.getChildrenFromFields()) {
			appendNodes(child, nodes, buffers);
		}
	}

}