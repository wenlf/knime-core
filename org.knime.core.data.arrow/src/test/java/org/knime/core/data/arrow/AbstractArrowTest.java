package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.preproc.PreProcTableStore;
import org.knime.core.data.preproc.PreProcessingConfig;
import org.knime.core.data.row.RowBatchUtils;
import org.knime.core.data.table.cache.CachedTableStore;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreConfig;

public class AbstractArrowTest {

	private static ArrowTableStoreFactory m_factory;

	@BeforeClass
	public static void startup() {
		m_factory = new ArrowTableStoreFactory();
	}

	@AfterClass
	public static void shutdown() throws Exception {
		m_factory.close();
	}

	public ColumnType<?, ?>[] createWideSchema(ColumnType<?, ?> type, int width) {
		final ColumnType<?, ?>[] types = new ColumnType<?, ?>[width];
		for (int i = 0; i < width; i++) {
			types[i] = type;
		}
		return types;
	}

	public TableStore createStore(TableStoreConfig config, ColumnType<?, ?>... schema) {
		try {
			return m_factory.create(schema, createTmpFile(), config);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public CachedTableStore cache(TableStore store) {
		return RowBatchUtils.cache(store);
	}

	public PreProcTableStore preproc(TableStore delegate, int... indices) {
		return new PreProcTableStore(delegate, new PreProcessingConfig() {
			@Override
			public int[] getDomainEnabledIndices() {
				return indices;
			}
		});
	}

	public TableStore createStore(int chunkSize, ColumnType<?, ?>... schema) {
		return createStore(new TableStoreConfig() {
			@Override
			public int getInitialChunkSize() {
				return chunkSize;
			}
		}, schema);
	}

	@SuppressWarnings("resource")
	public BufferAllocator newAllocator() {
		// Root closed by factory.
		return new RootAllocator().newChildAllocator("Test", 0, Long.MAX_VALUE);
	}

	// also deletes file on exit
	public File createTmpFile() throws IOException {
		// file
		final File f = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow").toFile();
		f.deleteOnExit();
		return f;
	}
}
