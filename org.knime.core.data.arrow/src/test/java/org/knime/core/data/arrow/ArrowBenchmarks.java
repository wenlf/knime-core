package org.knime.core.data.arrow;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.WriteTable;
import org.knime.core.data.table.cache.CachedTableStore;
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
public class ArrowBenchmarks extends AbstractArrowTest {

	private long m_numRows = 25000;
	private int m_chunkSize = 17;

	private CachedTableStore m_store;

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
		m_store = cache(createStore(m_chunkSize, DoubleType.INSTANCE, StringType.INSTANCE));
	}

	@TearDown(Level.Iteration)
	public void closeCurrentStore() throws Exception {
		m_store.close();
	}

	@Benchmark
	public void newTablesBenchmark() throws Exception {

		final WriteTable writeTable = TableUtils.createWriteTable(m_store);
		try (RowWriteCursor writeCursor = writeTable.getCursor()) {
			final DoubleWriteValue doubleWriteValue = (DoubleWriteValue) writeCursor.get(0);
			final StringWriteValue stringWriteValue = (StringWriteValue) writeCursor.get(1);
			for (long i = 0; i < m_numRows; i++) {
				writeCursor.fwd();
				doubleWriteValue.setDouble(i);
				stringWriteValue.setStringValue("Entry " + i % 15);
			}
		}

		final ReadTable readTable = TableUtils.createReadTable(m_store);
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

//	public static void main(String[] args) throws Exception {
//		final DoubleTest benchmarks = new DoubleTest();
//		startup();
//		for (int i = 0; i < 5; i++) {
//			System.out.println("\n Starting iteration... " + i);
//			System.out.println("Setting up store.");
//			benchmarks.identityTestWideTable();
//
//			System.out.println("Running identitiy test.");
//			benchmarks.newTablesBenchmark();
//
//			System.out.println("Deleting identitiy test.");
//			benchmarks.closeCurrentStore();
//		}
//		System.out.println("Done");
//		shutdown();
//	}

	public static void main(String[] args) throws Exception {
		final DoubleTest benchmarks = new DoubleTest();
		startup();
		for (int i = 0; i < 5; i++) {
			System.out.println("Next... " + i);
			benchmarks.identityTestWideTable();
		}
		System.out.println("Done");
		shutdown();
	}
}
