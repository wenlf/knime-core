package org.knime.core.data.container.fast;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.TimerTask;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.store.TableReadStore;
import org.knime.core.data.table.store.TableStoreFactory;
import org.knime.core.data.table.store.TableStoreUtils;
import org.knime.core.internal.ReferencedFile;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.KNIMETimer;

/**
 * Fast table which is lazily loaded. Similar to 'ContainerTable' with delayed 'CopyTask'
 *
 * @author Christian Dietz, KNIME GmbH
 * @since 4.2
 */
class LazyFastTable extends AbstractFastTable {

    private final long m_size;

    private final TableStoreFactory m_factory;

    private final ColumnType<?, ?>[] m_types;

    private ReferencedFile m_fileRef;

    private CopyOnAccessTask m_readTask;

    private TableReadStore m_store;

    private IDataRepository m_repository;

    LazyFastTable(final IDataRepository repository, final ReferencedFile fileRef, final int tableId,
        final DataTableSpec spec, final TableStoreFactory factory, final long size, final boolean isRowKeys) {
        super(tableId, spec, isRowKeys);
        m_types = /* TODO Is this mapping always valid? do things change over time with versions? Should the mapper be stored as well? */
            FastTables.getFastTableSpec(spec, isRowKeys);
        m_fileRef = fileRef;
        m_factory = factory;
        m_size = size;
        m_repository = repository;
        m_readTask = new CopyOnAccessTask();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ensureOpen() {
        // TODO revise logic here, especially sync logic
        CopyOnAccessTask readTask = m_readTask;
        if (readTask == null) {
            return;
        }
        synchronized (m_readTask) {
            // synchronized may have blocked when another thread was
            // executing the copy task. If so, there is nothing else to
            // do here
            if (m_readTask == null) {
                return;
            }
            try {
                m_store = m_readTask.createTableStore();
            } catch (IOException i) {
                throw new RuntimeException(
                    "Exception while accessing file: \"" + m_fileRef.getFile().getName() + "\": " + i.getMessage(), i);
            }
            m_readTask = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        // just close the store. no need to clean anything up
        try {
            // TODO required?
            if (m_store != null) {
                m_store.close();
            }
        } catch (Exception ex) {
        }

        if (m_readTask != null) {
            IDataRepository dataRepository = CheckUtils.checkArgumentNotNull(m_repository);
            dataRepository.removeTable(getTableId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableReadStore getStore() {
        ensureOpen();
        return m_store;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return m_store != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() {
        ensureOpen();
        return m_store.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableRowIterator iterator() {
        ensureOpen();
        // TODO we don't want to recreate a table per iterator...
        final ReadTable table = TableStoreUtils.createReadTable(m_store);
        return new FastTableRowReader(table.newCursor(), m_spec, m_isRowKey);
    }

    @Override
    public CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        ensureOpen();
        // TODO implement row index selection as RowBatchReaderConfig (start at...)
        final ReadTable table = TableStoreUtils.createReadTable(m_store, FastTables.create(filter));
        return new FastTableRowReader(table.newCursor(), m_spec, m_isRowKey);
    }

    @Override
    public void saveToFile(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        throw new IllegalStateException("Why should I save an already saved table again?");
    }

    final class CopyOnAccessTask {

        private final NodeLogger LOGGER = NodeLogger.getLogger(CopyOnAccessTask.class);

        /**
         * Delay im ms until copying process is reported to LOGGER, small files won't report their copying (if faster
         * than this threshold).
         */
        private static final long NOTIFICATION_DELAY = 3000;

        CopyOnAccessTask() {
        }

        /**
         * Called to start the copy process. Is only called once.
         *
         * @return The buffer instance reading from the temp file.
         * @throws IOException If the file can't be accessed.
         */
        TableReadStore createTableStore() throws IOException {
            // timer task which prints a INFO message that the copying
            // is in progress.
            TimerTask timerTask = null;
            m_fileRef.lock();
            try {
                final File file = m_fileRef.getFile();
                timerTask = new TimerTask() {
                    /** {@inheritDoc} */
                    @Override
                    public void run() {
                        double sizeInMB = file.length() / (double)(1 << 20);
                        String size = NumberFormat.getInstance().format(sizeInMB);
                        LOGGER.debug(
                            "Extracting data file \"" + file.getAbsolutePath() + "\" to temp dir (" + size + "MB)");
                    }
                };
                KNIMETimer.getInstance().schedule(timerTask, NOTIFICATION_DELAY);
                // TODO why do we have to copy if we make sure we don't delete?
                return m_factory.create(m_types, file, m_size);
            } finally {
                if (timerTask != null) {
                    timerTask.cancel();
                }
                m_fileRef.unlock();
            }
        }
    }
}