package org.knime.core.data.arrow;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowBatchUtils;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.cache.CachedTableStore;
import org.knime.core.data.table.store.TableStoreFactory;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringType;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.DoubleWriteValue;
import org.knime.core.data.value.StringReadValue;
import org.knime.core.data.value.StringWriteValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
//TODO create extra project for benchmarks: org.knime.core.data.benchmark
public class ArrowBenchmarks {

	public static final long NUM_ROWS = 100_000_00;

	public static final long OFFHEAP_SIZE_IN_BYTES = 4_000_000_000l;

	public static final int BATCH_SIZE = 64_000;

	private ColumnType<?, ?>[] m_schema = new ColumnType[] { DoubleType.INSTANCE, StringType.INSTANCE };

	private CachedTableStore m_store;

	private RowBatchFactory m_factory;

	private TableStoreFactory m_format;

	public TableStoreFactory createFormat() {
		return new ArrowTableStoreFactory(BATCH_SIZE);
	}

	public static Options jmhOptions(final String className) {
		return new OptionsBuilder().include(className)//
				.forks(0)//
				.warmupIterations(5)//
				.measurementIterations(10)//
				.shouldDoGC(true)//
				.build();
	}

	@Test
	public void test() throws RunnerException {
		new Runner(jmhOptions(this.getClass().getSimpleName())).run();
	}

	@Setup(Level.Iteration)
	public void setupNextStore() throws IOException {
		m_store = RowBatchUtils.cache(m_format.create(m_schema, createTmpFile(), null));
		m_factory = m_store.createFactory();
	}

	@Setup(Level.Trial)
	public void setupFormat() {
		m_format = createFormat();
	}

	@TearDown(Level.Iteration)
	public void closeCurrentStore() throws Exception {
		m_store.close();
	}

	@TearDown(Level.Trial)
	public void destroyFormat() throws Exception {
		m_format.close();
	}

	@Benchmark
	public void newTablesBenchmark() throws Exception {
		final WriteTable writeTable = TableUtils.createWriteTable(m_store);
		try (RowWriteCursor writeCursor = writeTable.getCursor()) {
			final DoubleWriteValue doubleWriteValue = (DoubleWriteValue) writeCursor.get(0);
			final StringWriteValue stringWriteValue = (StringWriteValue) writeCursor.get(1);
			for (long i = 0; i < NUM_ROWS; i++) {
				writeCursor.fwd();
				doubleWriteValue.setDouble(i);
				stringWriteValue.setStringValue("Entry " + i % 15);
			}
		}

		final ReadTable readTable = TableUtils.createReadTable(m_schema, m_store, new RowBatchReaderConfig() {
			@Override
			public int[] getColumnIndices() {
				// TODO if null, all.
				return new int[] { 0, 1 };
			}
		});
		try (RowReadCursor readCursor = readTable.newCursor()) {
			DoubleReadValue doubleReadValue = (DoubleReadValue) readCursor.get(0);
			StringReadValue stringReadValue = (StringReadValue) readCursor.get(1);
			long i = 0;
			while (readCursor.canFwd()) {
				readCursor.fwd();
				assertEquals("Entry " + i % 15, stringReadValue.getStringValue());
				assertEquals(doubleReadValue.getDouble(), i++, 0.000000001);
			}
		}
	}

	private File createTmpFile() throws IOException {
		// file
		final File f = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow").toFile();
		f.deleteOnExit();
		return f;
	}

	public static void main(String[] args) throws Exception {
		final ArrowBenchmarks benchmarks = new ArrowBenchmarks();
		benchmarks.setupFormat();
		for (int i = 0; i < 5; i++) {
			System.out.println("\n Starting iteration... " + i);
			System.out.println("Setting up store.");
			benchmarks.setupNextStore();

			System.out.println("Running identitiy test.");
			benchmarks.newTablesBenchmark();

			System.out.println("Deleting identitiy test.");
			benchmarks.closeCurrentStore();
		}
		System.out.println("Done");
		benchmarks.destroyFormat();
	}
}
