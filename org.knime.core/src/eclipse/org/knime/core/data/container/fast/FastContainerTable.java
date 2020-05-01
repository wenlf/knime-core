/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 1, 2020 (dietzc): created
 */
package org.knime.core.data.container.fast;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.row.RowBatchReaderConfig;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.StringReadValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTable.KnowsRowCountTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.WorkflowDataRepository;

/**
 *
 * @author dietzc
 */
public class FastContainerTable implements ContainerTable {

    private final static BufferedDataTable[] EMPTY_ARRAY = new BufferedDataTable[0];

    private TableStore m_store;

    private final DataTableSpec m_spec;

    private final int m_id;

    /**
     * @param spec
     * @param store
     */
    public FastContainerTable(final int id, final DataTableSpec spec, final TableStore store) {
        m_store = store;
        m_spec = spec;
        m_id = id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRowCount() {
        return KnowsRowCountTable.checkRowCount(size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() {
        return m_store.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveToFile(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // TODO
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        // TODO this means we kill a table. store.close()? store.clear()?
        m_store = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ensureOpen() {
        // TODO copy file over to tmp
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableRowIterator iterator() {
        // TODO we don't want to recreate a table per iterator...
        final ReadTable table = TableUtils.createReadTable(m_store);
        return new FastTableRowReader(table.newCursor(), m_spec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        // TODO implement row index selection as RowBatchReaderConfig (start at...)
        final ReadTable table = TableUtils.createReadTable(m_store, new RowBatchReaderConfig() {
            @Override
            public int[] getColumnIndices() {
                Optional<Set<Integer>> materializeColumnIndices = filter.getMaterializeColumnIndices();
                final int[] selected;
                if (materializeColumnIndices.isPresent()) {
                    final List<Integer> indices = new ArrayList<>(materializeColumnIndices.get());
                    Collections.sort(indices);
                    selected = new int[indices.size()];
                    for (int i = 0; i < selected.length; i++) {
                        selected[i] = indices.get(i);
                    }
                } else {
                    selected = null;
                }
                return selected;
            }
        });
        return new FastTableRowReader(table.newCursor(), m_spec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BufferedDataTable[] getReferenceTables() {
        return EMPTY_ARRAY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putIntoTableRepository(final WorkflowDataRepository dataRepository) {
        dataRepository.addTable(getTableId(), this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeFromTableRepository(final WorkflowDataRepository dataRepository) {
        dataRepository.removeTable(m_id);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataTableSpec getDataTableSpec() {
        return m_spec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTableId() {
        return m_id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return m_store != null;
    }

    class FastTableRowReader extends CloseableRowIterator {

        private final RowReadCursor m_cursor;

        private StringReadValue m_rowKeySupplier;

        private final int m_numColumns;

        private DataValueSupplier<?>[] m_suppliers;

        private DataCell[] m_cells;

        private final DataCell MISSING = DataType.getMissingCell();

        public FastTableRowReader(final RowReadCursor cursor, final DataTableSpec spec) {
            m_cursor = cursor;
            m_numColumns = spec.getNumColumns();
            m_suppliers = new DataValueSupplier[spec.getNumColumns()];
            m_rowKeySupplier = cursor.get(0);
            m_cells = new DataCell[m_numColumns];

            // TODO do mapping once. we can use the same mapping for each reader.
            // TODO which mapping to use? output spec from knime or columntypes spec of store?
            for (int i = 0; i < m_suppliers.length; i++) {
                if (spec.getColumnSpec(i).getType() == DoubleCell.TYPE) {
                    m_suppliers[i] = new DoubleCellConsumer(cursor.get(i + 1));
                } else if (spec.getColumnSpec(i).getType() == StringCell.TYPE) {
                    m_suppliers[i] = new StringCellConsumer(cursor.get(i + 1));
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_cursor.canFwd();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            m_cursor.fwd();

            // we can reuse m_cells. default row copies anyway :-(
            for (int i = 0; i < m_cells.length; i++) {
                if (m_suppliers[i].isMissing()) {
                    m_cells[i] = MISSING;
                } else {
                    m_cells[i] = m_suppliers[i].get();
                }
            }

            // TODO how cool would it be to return a 'reusable DataRow proxy instead of generating millions of objects'.
            return new DefaultRow(m_rowKeySupplier.getStringValue(), m_cells);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            try {
                m_cursor.close();
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    /*
     * HELPERs. TODO EXTENSIBLE!!!
     */
    interface DataValueSupplier<D extends DataCell> extends Supplier<D> {
        boolean isMissing();
    }

    class DoubleCellConsumer implements DataValueSupplier<DoubleCell> {
        private final DoubleReadValue m_value;

        public DoubleCellConsumer(final DoubleReadValue value) {
            m_value = value;
        }

        @Override
        public DoubleCell get() {
            // TODO how cool would it be to just return a proxy DoubleValue:-(
            return new DoubleCell(m_value.getDouble());
        }

        @Override
        public boolean isMissing() {
            return m_value.isMissing();
        }

    }

    class StringCellConsumer implements DataValueSupplier<StringCell> {
        private final StringReadValue m_value;

        public StringCellConsumer(final StringReadValue value) {
            m_value = value;
        }

        @Override
        public StringCell get() {
            // TODO how cool would it be to just return a proxy DoubleValue:-(
            return new StringCell(m_value.getStringValue());
        }

        @Override
        public boolean isMissing() {
            return m_value.isMissing();
        }

    }

}
