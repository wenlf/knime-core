package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.util.TransferPair;

/* NB: This reader has best performance when data is accessed sequentially row-wise.
* TODO Maybe different flush / loader combinations are configurable per node later?
*/
public class FieldVectorReader implements AutoCloseable {

	// some constants
	private final BufferAllocator m_alloc;

	// Varies with each partition
	private VectorSchemaRoot m_root;

	private File m_file;

	private ArrowFileReader m_reader;

	private List<ArrowBlock> m_blocks;

	// TODO support for column filtering and row filtering ('TableFilter'), i.e.
	// only load required columns / rows from disc. Rows should be easily possible
	// by using 'ArrowBlock'
	// TODO maybe easier with parquet backend?
	public FieldVectorReader(final File file, final BufferAllocator alloc) throws IOException {
		m_alloc = alloc;
		m_file = file;
	}

	// Assumption for this reader: sequential loading.
	public FieldVector[] read(long index) throws IOException {
		initialize();
		// load next
		m_reader.loadRecordBatch(m_blocks.get((int) index));

		final List<FieldVector> fieldVectors = m_root.getFieldVectors();
		final FieldVector[] res = new FieldVector[fieldVectors.size()];
		for (int i = 0; i < res.length; i++) {
			final FieldVector v = fieldVectors.get(i);
			final TransferPair transferPair = v.getTransferPair(m_alloc);
			transferPair.transfer();
			res[i] = (FieldVector) transferPair.getTo();
		}
		return res;
	}

	@SuppressWarnings("resource")
	private void initialize() throws IOException {
		if (m_reader == null) {
			m_reader = new ArrowFileReader(new RandomAccessFile(m_file, "rw").getChannel(), m_alloc);
			m_root = m_reader.getVectorSchemaRoot();
			m_blocks = m_reader.getRecordBlocks();
		}
	}

	public int size() {
		try {
			initialize();
			return m_blocks.size();
		} catch (IOException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws Exception {
		if (m_root != null) {
			m_reader.close();
		}
		m_alloc.close();
	}
}
