package org.knime.core.data.container.fast;

import java.util.function.Supplier;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.KNIMEStreamConstants;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.row.RowReadCursor;
import org.knime.core.data.value.DoubleReadValue;
import org.knime.core.data.value.StringReadValue;

class FastTableRowReader extends CloseableRowIterator {

    private final RowReadCursor m_cursor;

    private final StringReadValue m_rowKeySupplier;

    private final int m_numColumns;

    private DataValueSupplier<?>[] m_suppliers;

    private DataCell[] m_cells;

    private final DataCell MISSING = DataType.getMissingCell();

    public FastTableRowReader(final RowReadCursor cursor, final DataTableSpec spec, final boolean isRowKey) {
        m_cursor = cursor;
        m_numColumns = spec.getNumColumns();
        m_suppliers = new DataValueSupplier[spec.getNumColumns()];
        m_cells = new DataCell[m_numColumns];

        int offset = 0;
        if (isRowKey) {
            m_rowKeySupplier = cursor.get(0);
            offset = 1;
        } else {
            m_rowKeySupplier = null;
        }
        // TODO do mapping once. we can use the same mapping for each reader.
        // TODO which mapping to use? output spec from knime or columntypes spec of store?
        for (int i = 0; i < m_suppliers.length; i++) {
            if (spec.getColumnSpec(i).getType() == DoubleCell.TYPE) {
                m_suppliers[i] = new DoubleCellConsumer(cursor.get(i + offset));
            } else if (spec.getColumnSpec(i).getType() == StringCell.TYPE) {
                m_suppliers[i] = new StringCellConsumer(cursor.get(i + offset));
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
        if (m_rowKeySupplier != null) {
            return new DefaultRow(m_rowKeySupplier.getStringValue(), m_cells);
        }else {
            return new DefaultRow(KNIMEStreamConstants.DUMMY_ROW_KEY, m_cells);
        }
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

    /*
     * HELPERs. TODO EXTENSIBLE!!! Versioned?
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