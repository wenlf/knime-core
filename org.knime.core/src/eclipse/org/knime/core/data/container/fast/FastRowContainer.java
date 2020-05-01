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

import java.util.function.Consumer;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.RowContainer;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.row.RowWriteCursor;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreUtils;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringType;
import org.knime.core.data.value.DoubleWriteValue;
import org.knime.core.data.value.StringWriteValue;

/**
 * Wrapper for DataContainer
 *
 * @author Christian Dietz, KNIME
 */
public class FastRowContainer implements RowContainer {

    private RowWriteCursor m_cursor;

    private DataCellConsumer<DataCell>[] m_consumers;

    private StringWriteValue m_rowKeyWriter;

    private TableStore m_store;

    private DataTableSpec m_spec;

    private TmpFastTable m_table;

    private final int m_tableId;

    private final boolean m_isRowKey;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public FastRowContainer(final int tableId, final DataTableSpec spec, final TableStore store,
        final boolean isRowKey) {
        m_cursor = TableStoreUtils.createWriteTable(store).getCursor();
        m_store = store;
        m_spec = spec;
        m_tableId = tableId;
        m_isRowKey = isRowKey;

        int offset = 0;
        if (m_isRowKey) {
            m_rowKeyWriter = m_cursor.get(0);
            offset = 1;
        } else {
            m_rowKeyWriter = null;
        }

        m_consumers = new DataCellConsumer[spec.getNumColumns()];
        final ColumnType<?, ?>[] columnTypes = store.getColumnTypes();
        for (int i = 0; i < m_consumers.length + 1; i++) {
            if (columnTypes[i] instanceof DoubleType) {
                m_consumers[i] = (DataCellConsumer)new DoubleCellConsumer(m_cursor.get(i + offset));
            } else if (columnTypes[i] instanceof StringType) {
                m_consumers[i] = (DataCellConsumer)new StringCellConsumer(m_cursor.get(i + offset));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (isClosed()) {
            return;
        } else {
            try {
                m_cursor.close();
            } catch (Exception ex) {
                // TODO OK?
                throw new IllegalStateException(ex);
            }
            m_table = new TmpFastTable(m_tableId, m_spec, m_store, m_isRowKey);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addRowToTable(final DataRow row) {
        m_cursor.fwd();
        m_rowKeyWriter.setStringValue(row.getKey().getString());
        for (int i = 0; i < row.getNumCells(); i++) {
            final DataCell cell = row.getCell(i);
            if (cell.isMissing()) {
                m_consumers[i + 1].setMissing();
            } else {
                m_consumers[i + 1].accept(cell);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerTable getTable() {
        if (!isClosed()) {
            // TODO
            throw new IllegalStateException("Close container before calling 'getTable()'");
        }
        return m_table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        m_table.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return m_table != null;
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
    public void setMaxPossibleValues(final int maxPossibleValues) {
        // TODO
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataTableSpec getTableSpec() {
        return m_spec;
    }

    /*
     * HELPERs. TODO EXTENSIBLE!!!
     */
    interface DataCellConsumer<D extends DataCell> extends Consumer<D> {
        void setMissing();
    }

    class DoubleCellConsumer implements DataCellConsumer<DoubleCell> {
        private final DoubleWriteValue m_value;

        public DoubleCellConsumer(final DoubleWriteValue value) {
            m_value = value;
        }

        @Override
        public void accept(final DoubleCell cell) {
            m_value.setDouble(cell.getDoubleValue());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setMissing() {
            m_value.setMissing();
        }
    }

    class StringCellConsumer implements DataCellConsumer<StringCell> {
        private final StringWriteValue m_value;

        public StringCellConsumer(final StringWriteValue value) {
            m_value = value;
        }

        @Override
        public void accept(final StringCell cell) {
            m_value.setStringValue(cell.getStringValue());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setMissing() {
            m_value.setMissing();
        }
    }

}
