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

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.arrow.ArrowTableStoreFactory;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.container.RowContainer;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.table.store.TableStore;
import org.knime.core.data.table.store.TableStoreConfig;
import org.knime.core.data.table.store.TableStoreFactory;
import org.knime.core.data.table.store.TableStoreUtils;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringType;
import org.knime.core.internal.ReferencedFile;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.workflow.WorkflowDataRepository;

/**
 * Manages fast tables creation, serialization/deserialization and store versioning.
 *
 * @author Christian Dietz, KNIME GmbH
 * @since 4.2
 */
public class FastTables {

    /**
     *
     */
    private static final String FAST_TABLE_CONTAINER_ROWKEY = "FAST_TABLE_CONTAINER_ROWKEY";

    private static final String FAST_TABLE_CONTAINER_SIZE = "FAST_TABLE_CONTAINER_SIZE";

    private static final String FAST_TABLE_CONTAINER_TYPE = "FAST_TABLE_CONTAINER_TYPE";

    // TODO extension point?
    // TODO sorted by 'priority'?
    private final static TableStoreFactory[] FACTORIES = new TableStoreFactory[]{new ArrowTableStoreFactory()};

    /**
     * @param fileRef
     * @param spec
     * @param id
     * @param dataRepository
     * @param settings
     * @return
     * @throws InvalidSettingsException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("resource")
    public static LazyFastTable readFromFileDelayed(final ReferencedFile fileRef, final DataTableSpec spec,
        final int id, final WorkflowDataRepository dataRepository, final NodeSettingsRO settings)
        throws InvalidSettingsException {
        try {
            final long size = settings.getLong(FAST_TABLE_CONTAINER_SIZE);
            final TableStoreFactory factory =
                (TableStoreFactory)Class.forName(settings.getString(FAST_TABLE_CONTAINER_TYPE)).newInstance();
            return new LazyFastTable(dataRepository, fileRef, id, spec, factory, size,
                settings.getBoolean(FAST_TABLE_CONTAINER_ROWKEY));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            // TODO we stored the wrong factory
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param delegate
     * @param outFile
     * @param s
     * @param exec
     * @throws CanceledExecutionException
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public static void saveToFile(final FastTable delegate, final File outFile, final NodeSettings s,
        final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
        // TODO AVOID cast.
        s.addString(FAST_TABLE_CONTAINER_TYPE, delegate.getStore().getFactory().getCanonicalName());
        s.addLong(FAST_TABLE_CONTAINER_SIZE, delegate.size());
        s.addBoolean(FAST_TABLE_CONTAINER_ROWKEY, delegate.isRowKeys());
        delegate.saveToFile(outFile, s, exec);
    }

    /**
     * Create a new {@link RowContainer} based on fast tables.
     *
     * @param tableId the table id. must be unique for a data repository.
     * @param spec spec to be mapped. Make sure to call {@link FastTables#isCompatible(DataTableSpec)} to ensure
     *            compatibility.
     * @param config configuration of the container
     * @param dest destination file used to serialize/deserialize data.
     * @return a {@link RowContainer}
     */
    @SuppressWarnings("resource")
    public static FastRowContainer create(final int tableId, final DataTableSpec spec, final FastTableConfig config,
        final File dest, final boolean isRowKey) {
        final TableStore store = TableStoreUtils
            .cache(FACTORIES[0].create(getFastTableSpec(spec, config.isRowKeyEnabled()), dest, new TableStoreConfig() {

                @Override
                public int getInitialChunkSize() {
                    // TODO make configurable
                    return 64000;
                }
            }));
        return new FastRowContainer(tableId, spec, store, isRowKey);
    }

    /**
     * Check if fast tables can be used with the {@link DataType}s of the {@link DataColumnSpec}s.
     *
     * @param spec to check compatibility
     * @return <code>true</code> if fast table is compatible to spec.
     */
    public static boolean isCompatible(final DataTableSpec spec) {
        // TODO more extensible!
        for (int i = 0; i < spec.getNumColumns(); i++) {
            if (!(spec.getColumnSpec(i).getType() == DoubleCell.TYPE
                || spec.getColumnSpec(i).getType() == StringCell.TYPE)) {
                return false;
            }
        }
        return true;
    }

    private static TableStoreFactory getFactoryByClass(final Class<?> class1) {
        for (TableStoreFactory factory : FACTORIES) {
            if (factory.getClass().isAssignableFrom(class1)) {
                return factory;
            }
        }
        throw new IllegalArgumentException("No factory of type " + class1.getSimpleName());
    }

    private static String getIdOfFactory(final TableStoreFactory factory) {
        final String canonical = factory.getClass().getCanonicalName();
        if (canonical == null) {
            // TODO better error messages
            throw new IllegalArgumentException(
                "Can't infer ID from factory " + factory.toString() + ". Maybe an anonymous class?");
        }
        return canonical;
    }

    private static TableStoreFactory getFactoryById(final String id) {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends TableStoreFactory> type = (Class<? extends TableStoreFactory>)Class.forName(id);
            return getFactoryByClass(type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't find factory with id " + id + ".");
        }
    }

    static ColumnType<?, ?>[] getFastTableSpec(final DataTableSpec spec, final boolean rowKey) {
        ColumnType<?, ?>[] mappedSpec = new ColumnType<?, ?>[spec.getNumColumns() + (rowKey ? 1 : 0)];
        if (rowKey) {
            // TODO introduce RowKeyType.INSTANCE
            mappedSpec[0] = StringType.INSTANCE;
        }
        for (int i = (rowKey ? 1 : 0); i < mappedSpec.length; i++) {
            mappedSpec[i] = map(spec.getColumnSpec(i - 1));
        }
        return mappedSpec;
    }

    private static ColumnType<?, ?> map(final DataColumnSpec columnSpec) {
        // TODO extensible
        final DataType type = columnSpec.getType();
        if (type == DoubleCell.TYPE) {
            return DoubleType.INSTANCE;
        } else if (type == StringCell.TYPE) {
            return StringType.INSTANCE;
        } else {
            throw new IllegalArgumentException("Can't derive spec for " + columnSpec.getName());
        }
    }

}
