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

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.arrow.ArrowTableStoreFactory;
import org.knime.core.data.column.ColumnType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.table.store.TableStoreFactory;
import org.knime.core.data.type.DoubleType;
import org.knime.core.data.type.StringType;
import org.knime.core.internal.ReferencedFile;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.workflow.WorkflowDataRepository;

/**
 * Manages fast tables serialization / deserialization with versioning.
 *
 * @author Christian Dietz, KNIME GmbH
 * @since 4.2
 */
public class FastTables {

    // TODO extension point?
    // TODO sorted by 'priority'
    private final static TableStoreFactory[] FACTORIES = new TableStoreFactory[]{new ArrowTableStoreFactory()};

    /**
     * @param fileRef
     * @param spec
     * @param id
     * @param dataRepository
     * @return
     */
    public static LazyFastTable readFromFileDelayed(final ReferencedFile fileRef, final DataTableSpec spec,
        final int id, final WorkflowDataRepository dataRepository) {
        // TODO load format factory if not already loaded (singleton.../ registry?)
        return null;
    }

    /**
     * @param delegate
     * @param outFile
     * @param s
     * @param exec
     */
    public static void saveToFile(final FastTable delegate, final File outFile, final NodeSettings s,
        final ExecutionMonitor exec) {
        // TODO store class of format factory for versioning

    }

    /**
     * @param spec
     * @param config
     * @return
     */
    public static FastRowContainer create(final DataTableSpec spec, final FastTableConfig config) {
        return null;
    }

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

    public static TableStoreFactory getFactoryByClass(final Class<?> class1) {
        for (TableStoreFactory factory : FACTORIES) {
            if (factory.getClass().isAssignableFrom(class1)) {
                return factory;
            }
        }
        throw new IllegalArgumentException("No factory of type " + class1.getSimpleName());
    }

    public static String getIdOfFactory(final TableStoreFactory factory) {
        final String canonical = factory.getClass().getCanonicalName();
        if (canonical == null) {
            // TODO better error messages
            throw new IllegalArgumentException(
                "Can't infer ID from factory " + factory.toString() + ". Maybe an anonymous class?");
        }
        return canonical;
    }

    public static TableStoreFactory getFactoryById(final String id) {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends TableStoreFactory> type = (Class<? extends TableStoreFactory>)Class.forName(id);
            return getFactoryByClass(type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't find factory with id " + id + ".");
        }
    }

    public static ColumnType<?, ?>[] getFastTableSpec(final DataTableSpec spec, final boolean rowKey) {
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
