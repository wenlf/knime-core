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
 *   Apr 30, 2020 (dietzc): created
 */
package org.knime.core.data.container;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.knime.core.data.IDataRepository;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;

/**
 * TODO docu
 *
 * @author Christian Dietz
 */
final class BufferRepositoryUtil {

    static final BufferMap EMPTY = new BufferMap() {
        @Override
        public void removeBuffer(final int id) {
            // noop
        }

        @Override
        public Optional<Buffer> getBuffer(final int id) {
            return Optional.empty();
        }
    };

    private BufferRepositoryUtil() {
    }

    static BufferRepository wrap(final IDataRepository delegate) {
        return new BufferRepository() {

            @Override
            public int generateNewID() {
                return delegate.generateNewID();
            }

            @Override
            public int getLastId() {
                return delegate.getLastId();
            }

            @Override
            public void updateLastId(final int id) {
                delegate.updateLastId(id);
            }

            @Override
            public void addTable(final int key, final ContainerTable table) {
                delegate.addTable(key, table);
            }

            @Override
            public Optional<ContainerTable> getTable(final int key) {
                return delegate.getTable(key);
            }

            @Override
            public Optional<ContainerTable> removeTable(final Integer key) {
                return delegate.removeTable(key);
            }

            @Override
            public void addFileStoreHandler(final IWriteFileStoreHandler handler) {
                delegate.addFileStoreHandler(handler);
            }

            @Override
            public void removeFileStoreHandler(final IWriteFileStoreHandler handler) {
                delegate.removeFileStoreHandler(handler);
            }

            @Override
            public IFileStoreHandler getHandler(final UUID storeHandlerUUID) {
                return delegate.getHandler(storeHandlerUUID);
            }

            @Override
            public IFileStoreHandler getHandlerNotNull(final UUID storeHandlerUUID) {
                return delegate.getHandlerNotNull(storeHandlerUUID);
            }

            @Override
            public void printValidFileStoreHandlersToLogDebug() {
                delegate.printValidFileStoreHandlersToLogDebug();
            }

            @Override
            public Optional<Buffer> getBuffer(final int bufferId) {
                final ContainerTable table = delegate.getTable(bufferId).orElse(null);
                if (table != null && table instanceof BufferedContainerTable) {
                    return Optional.of(((BufferedContainerTable)table).getBuffer());
                }
                return Optional.empty();
            }

            @Override
            public void removeBuffer(final int bufferId) {
                delegate.removeTable(bufferId);
            }
        };
    }

    static BufferMap wrap(final Map<Integer, ContainerTable> repository) {
        return new BufferMap() {

            @Override
            public Optional<Buffer> getBuffer(final int id) {
                ContainerTable table = repository.get(id);
                if (table != null && table instanceof BufferedContainerTable) {
                    return Optional.of(((BufferedContainerTable)table).getBuffer());
                }
                return Optional.empty();
            }

            @Override
            public void removeBuffer(final int id) {
                repository.remove(id);
            }
        };
    }

    /**
     *
     * @author Christian Dietz
     */
    interface BufferRepository extends IDataRepository, BufferMap {
        // NB Marker
    }

    /**
     *
     * @author dietzc
     */
    interface BufferMap {
        Optional<Buffer> getBuffer(int id);

        void removeBuffer(int id);
    }

}
