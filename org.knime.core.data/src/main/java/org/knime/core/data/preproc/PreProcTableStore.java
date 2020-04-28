package org.knime.core.data.preproc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.knime.core.data.column.ColumnChunk;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.domain.Domain;
import org.knime.core.data.domain.DomainCalculator;
import org.knime.core.data.row.RowBatch;
import org.knime.core.data.row.RowBatchFactory;
import org.knime.core.data.row.RowBatchReader;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowBatchWriter;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.type.DoubleDomainCalculator;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringDomainCalculator;
import org.knime.core.data.type.StringType;

// TODO generalize to arbitrary pre-processors with mergable results
public class PreProcTableStore implements TableStore {

	private final TableStore m_delegate;
	private final ColumnType<?, ?>[] m_columnTypes;
	private final int[] m_enabled;
	private final Map<Integer, Domain> m_results;

	private Map<ColumnType<?, ?>, DomainCalculator<?, ?>> m_calculators = new HashMap<>();

	private ExecutorService m_executors;
	private List<Future<Void>> m_futures;

	// TODO make extensible!!
	// TODO only put 'active' domains in the calculator map (e.g. in constructor)
	{
		m_calculators.put(DoubleType.INSTANCE, new DoubleDomainCalculator());
		// TODO make threshold configurable
		m_calculators.put(StringType.INSTANCE, new StringDomainCalculator(120));
	}

	public PreProcTableStore(TableStore delegate, PreProcessingConfig config) {
		m_delegate = delegate;
		m_columnTypes = delegate.getColumnTypes();
		m_results = new HashMap<Integer, Domain>();
		m_futures = new ArrayList<Future<Void>>();

		m_enabled = config.getDomainEnabledIndices();

		if (m_enabled != null && m_enabled.length > 0) {
			// TODO use something global from KNIME. ThreadFactory?
			m_executors = Executors.newFixedThreadPool(4);
			// initialize results
			for (int i = 0; i < m_enabled.length; i++) {
				final int current = m_enabled[i];
				m_results.put(current, m_calculators.get(m_columnTypes[current]).createEmpty());
			}
		}
	}

	@Override
	public RowBatchWriter getWriter() {
		final RowBatchWriter writer = m_delegate.getWriter();
		return new RowBatchWriter() {

			@Override
			public void close() throws Exception {
				writer.close();

				// TODO this is just because I'm too lazy to implement something smarter. For
				// now this guy is just making sure we've all domains updated and part of the
				// results...
				for (final Future<Void> future : m_futures) {
					future.get();
				}
				if (m_executors != null)
					m_executors.shutdown();
			}

			@Override
			public void write(RowBatch record) {
				/*
				 * TODO parallelize over columns or batches or both or selectable?
				 * 
				 * If we parallelize over columns we can make use of the assumption, that record
				 * batches always have the same columnar structure. however, open question: when
				 * do we actually pass the data to the cache? before domain calc has finished?
				 * 
				 * We want to optimize the node execution time, i.e. minimize the 'delay' until
				 * a downstream node can actually access the data. A downstream node can access
				 * the data if: - the entire table data is in the cache (in-memory case) or all
				 * data has been flushed and can be accessed from disc. - domain has been
				 * calculated on table level.
				 * 
				 * This means that adding data to the cache only helps if we can do work in the
				 * cache while the domain is being processed (e.g. flush).
				 */

				if (m_enabled != null && m_enabled.length > 0) {
					final ColumnChunk[] recordData = record.getRecordData();
					for (int i = 0; i < m_enabled.length; i++) {
						final int current = m_enabled[i];
						m_futures.add(m_executors.submit(new Callable<Void>() {
							@Override
							public Void call() throws Exception {
								@SuppressWarnings("unchecked")
								final DomainCalculator<ColumnChunk, Domain> domainCalculator = (DomainCalculator<ColumnChunk, Domain>) m_calculators
										.get(m_columnTypes[current]);
								final Domain result = domainCalculator.apply(recordData[current]);

								// TODO more fine-granular syncing!!!
								synchronized (m_results) {
									final Domain stored = m_results.get(current);
									m_results.put(current, domainCalculator.merge(result, stored));
								}
								return null;
							}
						}));
					}
				}

				/*
				 * TODO IDEA: Directly write column chunks to cache instead of record batch
				 * (Introduce 'ColumnStoreInterface' and make a single 'instance of' check in
				 * constructor)? Before we do that we need to investigate the performance
				 * benefits of that.
				 */
				writer.write(record);
			}

		};
	}

	@Override
	public RowBatchReader createReader(RowBatchReaderConfig config) {
		return m_delegate.createReader(config);
	}

	@Override
	public void close() throws Exception {
		m_delegate.close();
	}

	@Override
	public ColumnType<?, ?>[] getColumnTypes() {
		return m_delegate.getColumnTypes();
	}

	@Override
	public RowBatchFactory createFactory() {
		return m_delegate.createFactory();
	}

	public <D extends Domain> D getResultDomain(int index) {
		// TODO throw class cast exception etc.
		@SuppressWarnings("unchecked")
		final D casted = (D) m_results.get(index);
		return casted;
	}

	@Override
	public long size() {
		return m_delegate.size();
	}

}
