/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package ca.nrc.cadc.conformance.uws;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.xml.XmlUtil;
import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.HeadMethodWebRequest;
import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.junit.Assert;
import org.xml.sax.SAXException;

public abstract class AbstractUWSTest {

    private static Logger log = Logger.getLogger(AbstractUWSTest.class);

    private static final String UWS_SCHEMA_URL = "http://www.ivoa.net/xml/UWS/v1.0";
    private static final String UWS_SCHEMA_RESOURCE = "UWS-v1.0.xsd";
    private static final String XLINK_SCHEMA_URL = "http://www.w3.org/1999/xlink";
    private static final String XLINK_SCHEMA_RESOURCE = "XLINK.xsd";

    private static SAXBuilder parser;
    private static SAXBuilder validatingParser;

    protected static final int REQUEST_TIMEOUT = 300; // 5 minutes
    protected static String serviceUrl;
    //protected static String serviceSchema;
    protected static Level level;

    protected Map<String, String> schemaMap = new TreeMap<String, String>();

    static {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
        setServiceURL();
    }

    private static URI convertToURI(final String value, final String msg) {
        URI uri = null;

        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException bug) {
            throw new RuntimeException("BUG: invalid URI string", bug);
        } catch (NullPointerException bug) {
            throw new RuntimeException("BUG: null URI string", bug);
        }

        return uri;
    }

    private static void setServiceURL() {
        // get standardID from system property
        String standardIDName = AbstractUWSTest.class.getName() + ".standardID";
        String standardIDValue = System.getProperty(standardIDName);
        log.debug(standardIDName + "=" + standardIDValue);

        // get resourceIdentifier from system property
        String resourceIdentifierName = AbstractUWSTest.class.getName() + ".resourceIdentifier";
        String resourceIdentifierValue = System.getProperty(resourceIdentifierName);
        log.debug(resourceIdentifierName + "=" + resourceIdentifierValue);

        // if neither is set
        if (standardIDValue == null && resourceIdentifierValue == null) {
            // service to be tested is defined in system property
            serviceUrl = System.getProperty("service.url");
            if (serviceUrl == null) {
                throw new RuntimeException("service.url System property not set");
            }

            log.debug("serviceUrl: " + serviceUrl);
        } else {
            // else both standardID and resourceIdentifier must be defined in system property
            if (standardIDValue == null) {
                throw new RuntimeException("system property " + standardIDName + " not set");
            } else if (resourceIdentifierValue == null) {
                throw new RuntimeException("system property " + resourceIdentifierName + " not set");
            } else {
                // both are set, convert them to URI
                String msg = "system property " + standardIDName + " not set to valid standardID URI";
                URI standardID = convertToURI(standardIDValue, msg);
                msg = "system property " + resourceIdentifierName + " not set to valid UWS resourceIdentifier URI";
                URI resourceIdentifier = convertToURI(resourceIdentifierValue, msg);

                // get the service URL
                RegistryClient rc = new RegistryClient();
                URL tempUrl = rc.getServiceURL(resourceIdentifier, standardID, AuthMethod.ANON);
                if (tempUrl == null) {
                    throw new RuntimeException("No service URL found for resourceIdentifier="
                            + resourceIdentifier + ", standardID=" + standardID + ", AuthMethod=" + AuthMethod.ANON);
                } else {
                    serviceUrl = tempUrl.toExternalForm();
                }
            }
        }
    }

    public AbstractUWSTest() {
        String uwsSchemaUrl = XmlUtil.getResourceUrlString(UWS_SCHEMA_RESOURCE, AbstractUWSTest.class);
        log.debug("uwsSchemaUrl: " + uwsSchemaUrl);

        String xlinkSchemaUrl = XmlUtil.getResourceUrlString(XLINK_SCHEMA_RESOURCE, AbstractUWSTest.class);
        log.debug("xlinkSchemaUrl: " + xlinkSchemaUrl);

        schemaMap.put(UWS_SCHEMA_URL, uwsSchemaUrl);
        schemaMap.put(XLINK_SCHEMA_URL, xlinkSchemaUrl);

        parser = XmlUtil.createBuilder(null);
        /*
        parser = new SAXBuilder(new XMLReaderSAX2Factory(false, PARSER));
        parser.setProperty("http://apache.org/xml/properties/schema/external-schemaLocation", 
                "http://www.ivoa.net/xml/UWS/v1.0 " + serviceSchema
                + " http://www.w3c.org/1999/xlink " + xlinkSchema);
         */

        validatingParser = XmlUtil.createBuilder(schemaMap);
        /*
        validatingParser = new SAXBuilder(new XMLReaderSAX2Factory(true, PARSER));
        validatingParser.setFeature("http://xml.org/sax/features/validation", true);
        validatingParser.setFeature("http://apache.org/xml/features/validation/schema", true);
        validatingParser.setFeature("http://apache.org/xml/features/validation/schema-full-checking", true);
        validatingParser.setProperty("http://apache.org/xml/properties/schema/external-schemaLocation",
                "http://www.ivoa.net/xml/UWS/v1.0 " + serviceSchema + );
         */
    }

    protected Document buildDocument(String xml, boolean validate)
            throws IOException, JDOMException {
        if (validate) {
            return validatingParser.build(new StringReader(xml));
        } else {
            return parser.build(new StringReader(xml));
        }
    }

    protected String urlToString(String urlString)
            throws MalformedURLException, IOException {
        URL url = new URL(urlString);
        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }

    protected String createJob(WebConversation conversation)
            throws IOException, SAXException, JDOMException {
        log.debug("**************************************************");
        log.debug("HTTP POST: " + serviceUrl);

        // Create a new Job with default values.
        WebRequest postRequest = new PostMethodWebRequest(serviceUrl);

        return createJob(conversation, postRequest);
    }

    protected String createJob(WebConversation conversation, String xml)
            throws IOException, SAXException, JDOMException {
        log.debug("**************************************************");
        log.debug("HTTP POST: " + serviceUrl);

        // Create a new Job.
        ByteArrayInputStream in = new ByteArrayInputStream(xml.getBytes());
        WebRequest postRequest = new PostMethodWebRequest(serviceUrl, in, "text/xml");

        log.debug("Posted xml: " + xml);

        return createJob(conversation, postRequest);
    }

    protected String createJob(WebConversation conversation, String contentType, String content)
            throws IOException, SAXException, JDOMException {
        log.debug("**************************************************");
        log.debug("HTTP POST: " + serviceUrl);

        // Create a new Job.
        ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());
        WebRequest postRequest = new PostMethodWebRequest(serviceUrl, in, contentType);

        log.debug("Posted content: " + content);

        return createJob(conversation, postRequest);
    }

    protected String createJob(WebConversation conversation, Map<String, List<String>> parameters)
            throws IOException, SAXException, JDOMException {
        log.debug("**************************************************");
        log.debug("HTTP POST: " + serviceUrl);

        // Create a new Job with default values.
        WebRequest postRequest = new PostMethodWebRequest(serviceUrl);

        // Add parameters if available.
        if (parameters != null) {
            List<String> valueList;

            List<String> keyList = new ArrayList<String>(parameters.keySet());
            for (String key : keyList) {
                valueList = parameters.get(key);
                postRequest.setParameter(key, valueList.toArray(new String[0]));
            }
        }

        // Set the RUNID to INTTEST
        postRequest.setParameter("runId", new String[]{"INTTEST"});

        log.debug(Util.getRequestParameters(postRequest));

        return createJob(conversation, postRequest);
    }

    protected String createJob(WebConversation conversation, WebRequest request)
            throws IOException, SAXException, JDOMException {
        WebResponse response = conversation.getResponse(request);
        Assert.assertNotNull("POST response to " + serviceUrl + " is null", response);

        log.debug(Util.getResponseHeaders(response));

        log.debug("Response code: " + response.getResponseCode());
        Assert.assertEquals("POST response code to " + serviceUrl + " should be 303", 303, response.getResponseCode());

        // Get the redirect.
        String location = response.getHeaderField("Location");
        log.debug("Location: " + location);
        Assert.assertNotNull("POST response to " + serviceUrl + " location header not set", location);

        // Parse the jobId from the redirect URL.
        URL locationUrl = new URL(location);
        String path = locationUrl.getPath();
        String[] paths = path.split("/");
        String jobId = paths[paths.length - 1];

        log.debug("jobId: " + jobId);
        Assert.assertNotNull("jobId not found", jobId);

        // Follow the redirect.
        response = get(conversation, location);

        // Validate the XML against the schema and get a DOM Document.
        log.debug("XML:\r\n" + response.getText());
        Document document = buildDocument(response.getText(), true);

        Element root = document.getRootElement();
        Assert.assertNotNull("XML returned from GET of " + location + " missing root element", root);
        Namespace namespace = root.getNamespace();

        // Job should have exactly one Execution Phase.
        List list = root.getChildren("phase", namespace);
        Assert.assertEquals("XML returned from GET of " + location + " missing uws:phase element", 1, list.size());

        // Job should have exactly one Execution Duration.
        list = root.getChildren("executionDuration", namespace);
        Assert.assertEquals("XML returned from GET of " + location + " missing uws:executionDuration element", 1, list.size());

        // Job should have exactly one Deletion Time.
        list = root.getChildren("destruction", namespace);
        Assert.assertEquals("XML returned from GET of " + location + " missing uws:destruction element", 1, list.size());

        // Job should have exactly one Quote.
        list = root.getChildren("quote", namespace);
        Assert.assertEquals("XML returned from GET of " + location + " missing uws:quote element", 1, list.size());

        // Job should have exactly one Results List.
        list = root.getChildren("results", namespace);
        Assert.assertEquals("XML returned from GET of " + location + " missing uws:results element", 1, list.size());

        // Job should have zero or one Error.
        list = root.getChildren("error", namespace);
        Assert.assertTrue("XML returned from GET of " + location + " invalid number of uws:error elements", list.size() == 0
                || list.size() == 1);

        return jobId;
    }

    protected WebResponse head(WebConversation conversation, String resourceUrl)
            throws IOException, SAXException {
        log.debug("**************************************************");
        log.debug("HTTP HEAD: " + resourceUrl);
        WebRequest getRequest = new HeadMethodWebRequest(resourceUrl);
        conversation.clearContents();
        WebResponse response = conversation.getResponse(getRequest);
        Assert.assertNotNull("HEAD response to " + resourceUrl + " is null", response);

        log.debug(Util.getResponseHeaders(response));

        log.debug("Response code: " + response.getResponseCode());
        Assert.assertEquals("Non-200 GET response code to " + resourceUrl, 200, response.getResponseCode());

        return response;
    }

    protected WebResponse get(WebConversation conversation, String resourceUrl)
            throws IOException, SAXException {
        return get(conversation, resourceUrl, "text/xml"); // for UWS job resources
    }

    protected WebResponse get(WebConversation conversation, String resourceUrl, String expectedContentType)
            throws IOException, SAXException {
        log.debug("**************************************************");
        log.debug("HTTP GET: " + resourceUrl);
        WebRequest getRequest = new GetMethodWebRequest(resourceUrl);
        conversation.clearContents();
        WebResponse response = conversation.getResponse(getRequest);
        Assert.assertNotNull("GET response to " + resourceUrl + " is null", response);

        log.debug(Util.getResponseHeaders(response));

        int numRedir = 0;
        int rcode = response.getResponseCode();
        while (rcode == 302 || rcode == 303) {
            log.debug("Response code: " + rcode);
            String loc = response.getHeaderField("Location");
            Assert.assertNotNull("Location", loc);
            getRequest = new GetMethodWebRequest(loc);
            response = conversation.getResponse(getRequest);
            rcode = response.getResponseCode();
        }

        log.debug("Content-Type: " + response.getContentType());
        Assert.assertEquals("GET response Content-Type header to " + resourceUrl + " is incorrect", expectedContentType, response.getContentType());

        return response;
    }

    protected WebResponse post(WebConversation conversation, WebRequest request)
            throws IOException, SAXException {
        // POST request to the phase resource.
        log.debug("**************************************************");
        log.debug("HTTP POST: " + request.getURL().toString());
        log.debug(Util.getRequestParameters(request));

        conversation.clearContents();
        WebResponse response = conversation.getResponse(request);
        Assert.assertNotNull("POST response to " + request.getURL().toString() + " is null", response);

        log.debug(Util.getResponseHeaders(response));

        log.debug("Response code: " + response.getResponseCode());
        Assert.assertEquals("POST response code to " + request.getURL().toString() + " should be 303", 303, response.getResponseCode());

        // Get the redirect.
        String location = response.getHeaderField("Location");
        log.debug("Location: " + location);
        Assert.assertNotNull("POST response to " + request.getURL().toString() + " location header not set", location);
        //Assert.assertEquals(POST response to " + resourceUrl + " location header incorrect", baseUrl + "/" + jobId, location);

        return response;
    }

    /*
     * Default Job deletion uses an HTTP POST request.
     */
    protected WebResponse deleteJob(WebConversation conversation, String jobId)
            throws IOException, SAXException, JDOMException {
        return deleteJobWithDeleteRequest(conversation, jobId);
    }

    /*
     * Delete a job using an HTTP POST request.
     */
    protected WebResponse deleteJobWithPostRequest(WebConversation conversation, String jobId)
            throws IOException, SAXException, JDOMException {
        String resourceUrl = serviceUrl + "/" + jobId;
        log.debug("**************************************************");
        log.debug("HTTP POST: " + resourceUrl);
        WebRequest postRequest = new PostMethodWebRequest(resourceUrl);
        postRequest.setParameter("ACTION", "DELETE");
        return deleteJobImpl(conversation, postRequest, resourceUrl);
    }

    /*
     * Delete a Job using a HTTP DELETE request.
     */
    protected WebResponse deleteJobWithDeleteRequest(WebConversation conversation, String jobId)
            throws IOException, SAXException, JDOMException {
        String resourceUrl = serviceUrl + "/" + jobId;
        log.debug("**************************************************");
        log.debug("HTTP DELETE: " + resourceUrl);
        WebRequest deleteRequest = new DeleteMethodWebRequest(resourceUrl);
        return deleteJobImpl(conversation, deleteRequest, resourceUrl);
    }

    private WebResponse deleteJobImpl(WebConversation conversation, WebRequest request, String resourceUrl)
            throws IOException, SAXException, JDOMException {
        log.debug(Util.getRequestParameters(request));

        conversation.clearContents();
        WebResponse response = conversation.getResponse(request);
        Assert.assertNotNull("Response to request is null", response);

        log.debug(Util.getResponseHeaders(response));

        log.debug("response code: " + response.getResponseCode());
        Assert.assertEquals("Response code should be 303", 303, response.getResponseCode());

        // Get the redirect.
        String location = response.getHeaderField("Location");
        log.debug("Location: " + location);
        Assert.assertNotNull("Response location header not set", location);
        Assert.assertEquals("Response to location header incorrect", serviceUrl, location);

        return response;
    }

}
