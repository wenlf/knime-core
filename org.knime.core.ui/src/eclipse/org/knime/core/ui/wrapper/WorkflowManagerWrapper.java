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
 *   Oct 13, 2016 (hornm): created
 */
package org.knime.core.ui.wrapper;

import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.dialog.ExternalNodeData;
import org.knime.core.node.port.MetaPortInfo;
import org.knime.core.node.workflow.ConnectionID;
import org.knime.core.node.workflow.EditorUIInformation;
import org.knime.core.node.workflow.NodeAnnotation;
import org.knime.core.node.workflow.NodeID;
import org.knime.core.node.workflow.NodeMessage;
import org.knime.core.node.workflow.NodeMessage.Type;
import org.knime.core.node.workflow.NodeUIInformation;
import org.knime.core.node.workflow.NodeUIInformationEvent;
import org.knime.core.node.workflow.WorkflowAnnotation;
import org.knime.core.node.workflow.WorkflowContext;
import org.knime.core.node.workflow.WorkflowListener;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.ui.node.workflow.ConnectionContainerUI;
import org.knime.core.ui.node.workflow.NodeContainerUI;
import org.knime.core.ui.node.workflow.WorkflowInPortUI;
import org.knime.core.ui.node.workflow.WorkflowManagerUI;
import org.knime.core.ui.node.workflow.WorkflowOutPortUI;
import org.knime.core.util.Pair;

/**
 * Implements the {@link WorkflowManagerUI} interface by simply wrapping the {@link WorkflowManager} implementation.
 *
 * For all return types that implement an interface (from core.api), another wrapper instance is returned.
 *
 * @author Martin Horn, University of Konstanz
 */
public final class WorkflowManagerWrapper extends NodeContainerWrapper<WorkflowManager> implements WorkflowManagerUI{

    /**
     * @param delegate the wfm to delegate all the calls to
     */
    private WorkflowManagerWrapper(final WorkflowManager delegate) {
        super(delegate);
    }

    /**
     * Wraps the object via {@link Wrapper#wrapOrGet(Object, java.util.function.Function)}.
     *
     * @param wfm the object to be wrapped
     * @return a new wrapper or a already existing one
     */
    public static final WorkflowManagerWrapper wrap(final WorkflowManager wfm) {
        return (WorkflowManagerWrapper)Wrapper.wrapOrGet(wfm, o -> new WorkflowManagerWrapper(o));
    }

    @Override
    public ReentrantLock getReentrantLockInstance() {
        return unwrap().getReentrantLockInstance();
    }

    @Override
    public boolean isLockedByCurrentThread() {
        return unwrap().isLockedByCurrentThread();
    }

    @Override
    public WorkflowManagerUI getProjectWFM() {
        return WorkflowManagerWrapper.wrap(unwrap().getProjectWFM());
    }

    @Override
    public void removeProject(final NodeID id) {
        unwrap().removeProject(id);
    }

    @Override
    public boolean canRemoveNode(final NodeID nodeID) {
        return unwrap().canRemoveNode(nodeID);
    }

    @Override
    public boolean isProject() {
        return unwrap().isProject();
    }

    @Override
    public ConnectionContainerUI addConnection(final NodeID source, final int sourcePort, final NodeID dest, final int destPort) {
        return ConnectionContainerWrapper.wrap(unwrap().addConnection(source, sourcePort, dest, destPort));
    }

    @Override
    public boolean canAddConnection(final NodeID source, final int sourcePort, final NodeID dest, final int destPort) {
        return unwrap().canAddConnection(source, sourcePort, dest, destPort);
    }

    @Override
    public boolean canAddNewConnection(final NodeID source, final int sourcePort, final NodeID dest, final int destPort) {
        return unwrap().canAddNewConnection(source, sourcePort, dest, destPort);
    }

    @Override
    public boolean canRemoveConnection(final ConnectionContainerUI cc) {
        return unwrap().canRemoveConnection(Wrapper.unwrapCC(cc));
    }

    @Override
    public void removeConnection(final ConnectionContainerUI cc) {
        unwrap().removeConnection(Wrapper.unwrapCC(cc));
    }

    @Override
    public Set<ConnectionContainerUI> getOutgoingConnectionsFor(final NodeID id, final int portIdx) {
        return unwrap().getOutgoingConnectionsFor(id, portIdx).stream().map(cc -> ConnectionContainerWrapper.wrap(cc)).collect(Collectors.toSet());
    }

    @Override
    public Set<ConnectionContainerUI> getOutgoingConnectionsFor(final NodeID id) {
        return unwrap().getOutgoingConnectionsFor(id).stream().map(cc -> ConnectionContainerWrapper.wrap(cc)).collect(Collectors.toSet());
    }

    @Override
    public ConnectionContainerUI getIncomingConnectionFor(final NodeID id, final int portIdx) {
        return ConnectionContainerWrapper.wrap(unwrap().getIncomingConnectionFor(id, portIdx));
    }

    @Override
    public Set<ConnectionContainerUI> getIncomingConnectionsFor(final NodeID id) {
        return unwrap().getIncomingConnectionsFor(id).stream().map(cc -> ConnectionContainerWrapper.wrap(cc)).collect(Collectors.toSet());
    }

    @Override
    public ConnectionContainerUI getConnection(final ConnectionID id) {
        return ConnectionContainerWrapper.wrap(unwrap().getConnection(id));
    }

    @Override
    public MetaPortInfo[] getMetanodeInputPortInfo(final NodeID metaNodeID) {
        return unwrap().getMetanodeInputPortInfo(metaNodeID);
    }

    @Override
    public MetaPortInfo[] getMetanodeOutputPortInfo(final NodeID metaNodeID) {
        return unwrap().getMetanodeOutputPortInfo(metaNodeID);
    }

    @Override
    public MetaPortInfo[] getSubnodeInputPortInfo(final NodeID subNodeID) {
        return unwrap().getSubnodeInputPortInfo(subNodeID);
    }

    @Override
    public MetaPortInfo[] getSubnodeOutputPortInfo(final NodeID subNodeID) {
        return unwrap().getSubnodeOutputPortInfo(subNodeID);
    }

    @Override
    public void changeMetaNodeInputPorts(final NodeID subFlowID, final MetaPortInfo[] newPorts) {
        unwrap().changeMetaNodeInputPorts(subFlowID, newPorts);
    }

    @Override
    public void changeMetaNodeOutputPorts(final NodeID subFlowID, final MetaPortInfo[] newPorts) {
        unwrap().changeMetaNodeOutputPorts(subFlowID, newPorts);
    }

    @Override
    public void changeSubNodeInputPorts(final NodeID subFlowID, final MetaPortInfo[] newPorts) {
        unwrap().changeSubNodeInputPorts(subFlowID, newPorts);
    }

    @Override
    public void changeSubNodeOutputPorts(final NodeID subFlowID, final MetaPortInfo[] newPorts) {
        unwrap().changeSubNodeOutputPorts(subFlowID, newPorts);
    }

    @Deprecated
    @Override
    public void resetAll() {
        unwrap().resetAll();
    }

    @Override
    public void resetAndConfigureAll() {
        unwrap().resetAndConfigureAll();
    }

    @Override
    public void executeUpToHere(final NodeID... ids) {
        unwrap().executeUpToHere(ids);
    }

    @Override
    public boolean canReExecuteNode(final NodeID id) {
        return unwrap().canReExecuteNode(id);
    }

    @Override
    public void saveNodeSettingsToDefault(final NodeID id) {
        unwrap().saveNodeSettingsToDefault(id);
    }

    @Override
    public void executePredecessorsAndWait(final NodeID id) throws InterruptedException {
        unwrap().executePredecessorsAndWait(id);
    }

    @Override
    public String canExpandSubNode(final NodeID subNodeID) {
        return unwrap().canExpandSubNode(subNodeID);
    }

    @Override
    public String canExpandMetaNode(final NodeID wfmID) {
        return unwrap().canExpandMetaNode(wfmID);
    }

    @Override
    public String canCollapseNodesIntoMetaNode(final NodeID[] orgIDs) {
        return unwrap().canCollapseNodesIntoMetaNode(orgIDs);
    }

    @Override
    public boolean canResetNode(final NodeID nodeID) {
        return unwrap().canResetNode(nodeID);
    }

    @Override
    public boolean canResetContainedNodes() {
        return unwrap().canResetContainedNodes();
    }

    @Override
    public void resetAndConfigureNode(final NodeID id) {
        unwrap().resetAndConfigureNode(id);
    }

    @Override
    public boolean canConfigureNodes() {
        return unwrap().canConfigureNodes();
    }

    @Override
    public boolean canExecuteNodeDirectly(final NodeID nodeID) {
        return unwrap().canExecuteNodeDirectly(nodeID);
    }

    @Override
    public boolean canExecuteNode(final NodeID nodeID) {
        return unwrap().canExecuteNode(nodeID);
    }

    @Override
    public boolean canCancelNode(final NodeID nodeID) {
        return unwrap().canCancelNode(nodeID);
    }

    @Override
    public boolean canCancelAll() {
        return unwrap().canCancelAll();
    }

    @Override
    public boolean canSetJobManager(final NodeID nodeID) {
        return unwrap().canSetJobManager(nodeID);
    }

    @Override
    public void shutdown() {
        unwrap().shutdown();
    }

    @Override
    public boolean executeAllAndWaitUntilDone() {
        return unwrap().executeAllAndWaitUntilDone();
    }

    @Override
    public boolean executeAllAndWaitUntilDoneInterruptibly() throws InterruptedException {
        return unwrap().executeAllAndWaitUntilDoneInterruptibly();
    }

    @Override
    public boolean waitWhileInExecution(final long time, final TimeUnit unit) throws InterruptedException {
        return unwrap().waitWhileInExecution(time, unit);
    }

    @Override
    public boolean canExecuteAll() {
        return unwrap().canExecuteAll();
    }

    @Override
    public void executeAll() {
        unwrap().executeAll();
    }

    @Override
    public String printNodeSummary(final NodeID prefix, final int indent) {
        return unwrap().printNodeSummary(prefix, indent);
    }

    @Override
    public Collection<NodeContainerUI> getNodeContainers() {
        return unwrap().getNodeContainers().stream().map(nc -> wrap(nc)).collect(Collectors.toList());
    }

    @Override
    public Collection<ConnectionContainerUI> getConnectionContainers() {
        return unwrap().getConnectionContainers().stream().map(cc -> ConnectionContainerWrapper.wrap(cc)).collect(Collectors.toList());
    }

    @Override
    public NodeContainerUI getNodeContainer(final NodeID id) {
        return wrap(unwrap().getNodeContainer(id));
    }

    @Override
    public <T> T getNodeContainer(final NodeID id, final Class<T> subclass, final boolean failOnError) {
        return unwrap().getNodeContainer(id, subclass, failOnError);
    }

    @Override
    public boolean containsNodeContainer(final NodeID id) {
        return unwrap().containsNodeContainer(id);
    }

    @Override
    public boolean containsExecutedNode() {
        return unwrap().containsExecutedNode();
    }

    @Deprecated
    @Override
    public List<NodeMessage> getNodeErrorMessages() {
        return unwrap().getNodeErrorMessages();
    }

    @Override
    public List<Pair<String, NodeMessage>> getNodeMessages(final Type... types) {
        return unwrap().getNodeMessages(types);
    }

    @Override
    public boolean isWriteProtected() {
        return unwrap().isWriteProtected();
    }

    @Override
    public List<NodeID> getLinkedMetaNodes(final boolean recurse) {
        return unwrap().getLinkedMetaNodes(recurse);
    }

    @Override
    public boolean canUpdateMetaNodeLink(final NodeID id) {
        return unwrap().canUpdateMetaNodeLink(id);
    }

    @Override
    public boolean hasUpdateableMetaNodeLink(final NodeID id) {
        return unwrap().hasUpdateableMetaNodeLink(id);
    }

    @Override
    public void setWorkflowPassword(final String password, final String hint) throws NoSuchAlgorithmException {
        unwrap().setWorkflowPassword(password, hint);
    }

    @Override
    public boolean isUnlocked() {
        return unwrap().isUnlocked();
    }

    @Override
    public String getPasswordHint() {
        return unwrap().getPasswordHint();
    }

    @Override
    public boolean isEncrypted() {
        return unwrap().isEncrypted();
    }

    @Override
    public OutputStream cipherOutput(final OutputStream out) throws IOException {
        return unwrap().cipherOutput(out);
    }

    @Override
    public String getCipherFileName(final String fileName) {
        return unwrap().getCipherFileName(fileName);
    }

    @Override
    public void addListener(final WorkflowListener listener) {
        unwrap().addListener(listener);
    }

    @Override
    public void removeListener(final WorkflowListener listener) {
        unwrap().removeListener(listener);
    }

    @Override
    public void setAutoSaveDirectoryDirtyRecursivly() {
        unwrap().setAutoSaveDirectoryDirtyRecursivly();
    }

    @Override
    public void setName(final String name) {
        unwrap().setName(name);
    }

    @Override
    public boolean renameWorkflowDirectory(final String newName) {
        return unwrap().renameWorkflowDirectory(newName);
    }

    @Override
    public String getNameField() {
        return unwrap().getNameField();
    }

    @Override
    public void setEditorUIInformation(final EditorUIInformation editorInfo) {
        unwrap().setEditorUIInformation(editorInfo);
    }

    @Override
    public EditorUIInformation getEditorUIInformation() {
        return unwrap().getEditorUIInformation();
    }

    @Override
    public int getNrWorkflowIncomingPorts() {
        return unwrap().getNrWorkflowIncomingPorts();
    }

    @Override
    public int getNrWorkflowOutgoingPorts() {
        return unwrap().getNrWorkflowOutgoingPorts();
    }

    @Override
    public NodeOutPortWrapper getWorkflowIncomingPort(final int i) {
        return NodeOutPortWrapper.wrap(unwrap().getWorkflowIncomingPort(i));
    }

    @Override
    public NodeInPortWrapper getWorkflowOutgoingPort(final int i) {
        return NodeInPortWrapper.wrap(unwrap().getWorkflowOutgoingPort(i));
    }

    @Override
    public void setInPortsBarUIInfo(final NodeUIInformation inPortsBarUIInfo) {
        unwrap().setInPortsBarUIInfo(inPortsBarUIInfo);
    }

    @Override
    public void setOutPortsBarUIInfo(final NodeUIInformation outPortsBarUIInfo) {
        unwrap().setOutPortsBarUIInfo(outPortsBarUIInfo);
    }

    @Override
    public NodeUIInformation getInPortsBarUIInfo() {
        return unwrap().getInPortsBarUIInfo();
    }

    @Override
    public NodeUIInformation getOutPortsBarUIInfo() {
        return unwrap().getOutPortsBarUIInfo();
    }

    @Override
    public WorkflowInPortUI getInPort(final int index) {
        return WorkflowInPortWrapper.wrap(unwrap().getInPort(index));
    }

    @Override
    public WorkflowOutPortUI getOutPort(final int index) {
        return WorkflowOutPortWrapper.wrap(unwrap().getOutPort(index));
    }

    @Override
    public Collection<WorkflowAnnotation> getWorkflowAnnotations() {
        return unwrap().getWorkflowAnnotations();
    }

    @Override
    public void addWorkflowAnnotation(final WorkflowAnnotation annotation) {
        unwrap().addWorkflowAnnotation(annotation);
    }

    @Override
    public void bringAnnotationToFront(final WorkflowAnnotation annotation) {
        unwrap().bringAnnotationToFront(annotation);
    }

    @Override
    public void sendAnnotationToBack(final WorkflowAnnotation annotation) {
        unwrap().sendAnnotationToBack(annotation);
    }

    @Override
    public void nodeUIInformationChanged(final NodeUIInformationEvent evt) {
        unwrap().nodeUIInformationChanged(evt);
    }

    @Override
    public List<NodeAnnotation> getNodeAnnotations() {
        return unwrap().getNodeAnnotations();
    }

    @Override
    public <T> T castNodeModel(final NodeID id, final Class<T> cl) {
        return unwrap().castNodeModel(id, cl);
    }

    @Override
    public <T> Map<NodeID, T> findNodes(final Class<T> nodeModelClass, final boolean recurse) {
        return unwrap().findNodes(nodeModelClass, recurse);
    }

    @Override
    public NodeContainerWrapper findNodeContainer(final NodeID id) {
        return NodeContainerWrapper.wrap(unwrap().findNodeContainer(id));
    }

    @Override
    public Map<String, ExternalNodeData> getInputNodes() {
        return unwrap().getInputNodes();
    }

    @Override
    public void setInputNodes(final Map<String, ExternalNodeData> input) throws InvalidSettingsException {
        unwrap().setInputNodes(input);
    }

    @Override
    public Map<String, ExternalNodeData> getExternalOutputs() {
        return unwrap().getExternalOutputs();
    }

    @Override
    public void removeWorkflowVariable(final String name) {
        unwrap().removeWorkflowVariable(name);
    }

    @Override
    public WorkflowContext getContext() {
        return unwrap().getContext();
    }

    @Override
    public void notifyTemplateConnectionChangedListener() {
        unwrap().notifyTemplateConnectionChangedListener();
    }

}
