/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 4 $
*
************************************************************************
 */

package ca.nrc.cadc.uws;

import ca.nrc.cadc.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.DataConversionException;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

/**
 * Constructs a Job List from an XML source. This class is not thread safe but it is
 * re-usable so it can safely be used to sequentially parse multiple XML transfer
 * documents. The Job objects returned are only sparcely populated: the jobID and phase are
 * the only two attributes set, as per the definition of 'ShortJobDescription' in the XSD.
 */
public class JobListReader {

    private static Logger log = Logger.getLogger(JobListReader.class);

    private static final String uwsSchemaUrl;
    private static final String xlinkSchemaUrl;

    static {
        uwsSchemaUrl = XmlUtil.getResourceUrlString(UWS.UWS_XSD_FILE, JobListReader.class);
        log.debug("uwsSchemaUrl: " + uwsSchemaUrl);

        xlinkSchemaUrl = XmlUtil.getResourceUrlString(UWS.XLINK_XSD_FILE, JobListReader.class);
        log.debug("xlinkSchemaUrl: " + xlinkSchemaUrl);
    }

    private Map<String, String> schemaMap;
    private SAXBuilder docBuilder;
    private final DateFormat dateFormat;

    /**
     * Constructor. XML Schema validation is enabled by default.
     */
    public JobListReader() {
        this(true);
    }

    /**
     * Constructor. XML schema validation may be disabled, in which case the client
     * is likely to fail in horrible ways (e.g. NullPointerException) if it receives
     * invalid documents. However, performance may be improved.
     *
     * @param enableSchemaValidation
     */
    public JobListReader(boolean enableSchemaValidation) {
        if (enableSchemaValidation) {
            schemaMap = new HashMap<String, String>();
            schemaMap.put(UWS.UWS_NAMESPACE, uwsSchemaUrl);
            schemaMap.put(UWS.XLINK_NAMESPACE, xlinkSchemaUrl);
            log.debug("schema validation enabled");
        } else {
            log.debug("schema validation disabled");
        }

        this.docBuilder = XmlUtil.createBuilder(schemaMap);
        this.dateFormat = UWS.getDateFormat();
    }

    /**
     * Alternative constructor to pass in additional schemas used to valid the
     * documents being read. Passing in an empty Map enables schema validation with 
     * no additional schemas other than the default UWS and XLink schemas.
     *
     * @param schemas Map of schema namespace to resource.
     */
    public JobListReader(Map<String, String> schemas) {
        if (schemas == null) {
            throw new IllegalArgumentException("Map of schema namespace to resource cannot be null");
        }
        schemaMap = new HashMap<String, String>();
        schemaMap.put(UWS.UWS_NAMESPACE, uwsSchemaUrl);
        schemaMap.put(UWS.XLINK_NAMESPACE, xlinkSchemaUrl);
        if (!schemas.isEmpty()) {
            Set<Entry<String, String>> entries = schemas.entrySet();
            for (Entry<String, String> entry : entries) {
                schemaMap.put(entry.getKey(), entry.getValue());
                log.debug("added to SchemaMap: " + entry.getKey() + " = " + entry.getValue());
            }
        }
        log.debug("schema validation enabled");

        this.docBuilder = XmlUtil.createBuilder(schemaMap);
        this.dateFormat = UWS.getDateFormat();
    }

    public List<JobRef> read(InputStream in)
            throws JDOMException, IOException, ParseException {
        try {
            return read(new InputStreamReader(in, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported");
        }
    }

    public List<JobRef> read(Reader reader)
            throws JDOMException, IOException, ParseException {
        Document doc = docBuilder.build(reader);
        return parseJobList(doc);
    }

    private List<JobRef> parseJobList(Document doc)
            throws ParseException, DataConversionException {
        Element root = doc.getRootElement();
        List<Element> children = root.getChildren();
        Iterator<Element> childIterator = children.iterator();
        List<JobRef> jobs = new ArrayList<JobRef>();
        JobRef jobRef = null;
        Date creationTime = null;
        Attribute nil = null;
        String runID = null;
        String ownerID = null;
        while (childIterator.hasNext()) {
            Element next = childIterator.next();
            final String jobID = next.getAttributeValue("id");

            Element phaseElement = next.getChild(JobAttribute.EXECUTION_PHASE.getAttributeName(), UWS.NS);
            String phase = phaseElement.getValue();
            final ExecutionPhase executionPhase = ExecutionPhase.valueOf(phase);

            Element creationTimeElement = next.getChild(JobAttribute.CREATION_TIME.getAttributeName(), UWS.NS);
            if (creationTimeElement != null) {
                String time = creationTimeElement.getValue();
                creationTime = dateFormat.parse(time);
            }

            Element runIDElement = next.getChild(JobAttribute.RUN_ID.getAttributeName(), UWS.NS);
            if (runIDElement != null) {
                runID = runIDElement.getTextTrim();
            }

            Element ownerIDElement = next.getChild(JobAttribute.OWNER_ID.getAttributeName(), UWS.NS);
            ownerID = ownerIDElement.getTextTrim();

            jobRef = new JobRef(jobID, executionPhase, creationTime, runID, ownerID);
            jobs.add(jobRef);
        }

        return jobs;
    }

}
