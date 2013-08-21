/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2013
 *  University of Konstanz, Germany and
 *  KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 *   19.08.2013 (thor): created
 */
package org.knime.testing.core.ng;

import java.io.File;
import java.util.Collection;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Result writer that writes all test suites into a single file.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
class XMLResultFileWriter extends AbstractXMLResultWriter {
    private final File m_file;

    private long m_startTime, m_endTime;

    XMLResultFileWriter(final File file) throws TransformerConfigurationException, ParserConfigurationException {
        m_file = file;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeResult(final Collection<WorkflowTestResult> results) throws TransformerException {
        Document doc = m_docBuilder.newDocument();
        Element testSuites = doc.createElement("testsuites");
        doc.appendChild(testSuites);

        int runs = 0;
        int errors = 0;
        int failures = 0;
        for (WorkflowTestResult res : results) {
            runs += res.runCount();
            errors += res.errorCount();
            failures += res.failureCount();

            testSuites.appendChild(createTestsuiteElement(res, doc));

        }
        testSuites.setAttribute("name", "All tests");
        testSuites.setAttribute("time", Double.toString((m_endTime - m_startTime) / 1000.0));
        testSuites.setAttribute("tests", Integer.toString(runs));
        testSuites.setAttribute("errors", Integer.toString(errors));
        testSuites.setAttribute("failures", Integer.toString(failures));

        Source source = new DOMSource(doc);
        Result result = new StreamResult(m_file);
        m_serializer.transform(source, result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startSuites() {
        m_startTime = System.currentTimeMillis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endSuites() {
        m_endTime = System.currentTimeMillis();
    }
}
