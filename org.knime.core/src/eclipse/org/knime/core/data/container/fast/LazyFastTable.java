package org.knime.core.data.container.fast;

/**
 * Fast table which is lazily loaded. Similar to 'ContainerTable' with delayed 'CopyTask'
 *
 * @author Christian Dietz, KNIME GmbH
 * @since 4.2
 */
interface LazyFastTable extends FastTable {
    // NB: Marker
}