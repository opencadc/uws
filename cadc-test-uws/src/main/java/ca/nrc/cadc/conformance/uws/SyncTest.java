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

import ca.nrc.cadc.util.Log4jInit;
import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.HttpException;
import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 *
 * @author jburke
 */
public class SyncTest extends AbstractUWSTest {

    protected static TestPropertiesList testPropertiesList;
    protected static boolean printPropertiesFiles = true;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws", Level.INFO);
    }

    private static Logger log = Logger.getLogger(SyncTest.class);

    private static final Integer RC_RESULTS_STREAMED = 200;
    private static final Integer RC_RESULTS_REDIRECT = 303;
    private static final Integer REDIRECT_LIMIT = 10;
    private static final String CLASS_NAME = SyncTest.class.getSimpleName();

    public SyncTest() {
        super();
    }

    @Before
    public void before() {
        String propertiesDirectory = System.getProperty("properties.directory");
        if (propertiesDirectory == null) {
            Assert.fail("properties.directory System property not set");
        }

        try {
            testPropertiesList = new TestPropertiesList(propertiesDirectory, CLASS_NAME);
        } catch (IOException e) {
            log.error(e);
            Assert.fail(e.getMessage());
        }
    }

    /* Since the SyncTest class is already being used by existing tests, 
     * for backwards compatibility, an abstract method is not used to define 
     * buildRequest().
     */
    protected WebRequest buildRequest(final TestProperties properties)
            throws Exception {
        throw new UnsupportedOperationException("This method is to be implemented by a subclass.");
    }

    protected void setAuthentication(WebConversation conversation, final TestProperties properties) {
        String realm = null;
        String userid = null;
        String password = null;
        if (properties.preconditions != null) {
            if (properties.preconditions.containsKey("Realm")) {
                realm = properties.preconditions.get("Realm").get(0);
            }

            if (properties.preconditions.containsKey("Userid")) {
                userid = properties.preconditions.get("Userid").get(0);
            }

            if (properties.preconditions.containsKey("Password")) {
                password = properties.preconditions.get("Password").get(0);
            }
        }

        if (userid != null && password != null && realm != null) {
            conversation.setAuthentication(realm, userid, password);
        }
    }

    protected String getContentType(final TestProperties properties) {
        String contentType = null;
        if (properties.expectations != null) {
            if (properties.expectations.containsKey("Content-Type")) {
                contentType = properties.expectations.get("Content-Type").get(0);
            }
        }

        return contentType;
    }

    protected Integer getResponseCode(final TestProperties properties) {
        Integer responseCode = null;
        if (properties.expectations != null) {
            if (properties.expectations.containsKey("Response-Code")) {
                responseCode = Integer.valueOf(properties.expectations.get("Response-Code").get(0));
            }
        }

        return responseCode;
    }

    protected WebResponse getRedirectResponse(WebConversation conversation,
            WebRequest request, WebResponse response)
            throws IOException, SAXException {
        int count = 0;
        while ((response.getResponseCode() == RC_RESULTS_REDIRECT)
                && (count < REDIRECT_LIMIT)) {
            // Get the redirect.
            String location = response.getHeaderField("Location");
            log.debug("Location: " + location);
            Assert.assertNotNull("GET response to " + request.getURL().toString() + " location header not set", location);

            // Follow the redirect.
            log.debug("**************************************************");
            log.debug("HTTP GET: " + location);
            WebRequest getRequest = new GetMethodWebRequest(location);
            conversation.clearContents();
            response = conversation.getResponse(getRequest);
            Assert.assertNotNull("GET response to " + location + " is null", response);

            log.debug(Util.getResponseHeaders(response));
            log.debug("Response code: " + response.getResponseCode());

            count++;
        }

        if (count >= REDIRECT_LIMIT) {
            throw new RuntimeException("Too many redirects");
        }

        return response;
    }

    protected WebResponse execute(WebConversation conversation, final WebRequest request)
            throws IOException, SAXException {
        // execute the operation
        WebResponse response = conversation.getResponse(request);
        Assert.assertNotNull("POST response to " + request.getURL().toString() + " is null", response);
        log.debug(Util.getResponseHeaders(response));
        log.debug("Response code: " + response.getResponseCode());

        // handle potential redirects
        response = getRedirectResponse(conversation, request, response);
        return response;
    }

    protected void process(WebConversation conversation, final WebRequest request,
            final TestProperties properties)
            throws IOException, SAXException {
        try {
            // execute the operation
            WebResponse response = this.execute(conversation, request);
            if (response.getResponseCode() != RC_RESULTS_STREAMED) {
                Assert.fail("Non-200 POST response code to " + serviceUrl);
            }

            if (response != null) {
                // Get the response text.
                log.debug("Response text:\r\n" + response.getText());

                String contentType = response.getHeaderField("Content-Type");
                Assert.assertEquals("Content-Type", this.getContentType(properties), contentType);
            }
        } catch (HttpException ex) {
            Integer responseCode = this.getResponseCode(properties);
            if (properties.filename.contains("ERROR") || (responseCode != null)) {
                if (responseCode != null) {
                    // if response code is expected, verify it
                    Assert.assertEquals(responseCode.intValue(), ex.getResponseCode());
                }

                // old test files do not provide response codes
                log.debug("caught expected: " + ex);
            } else {
                // unexpected exception
                throw ex;
            }
        }
    }

    protected void doTest() {
        if (testPropertiesList.propertiesList.isEmpty()) {
            log.warn("no properties files for " + this.getClass().getSimpleName());
            return;
        }
        String fname = null;
        try {
            // For each properties file.
            for (TestProperties properties : testPropertiesList.propertiesList) {
                if (printPropertiesFiles) {
                    log.info("testing properties file: " + properties.filename);
                    log.debug("\r\n" + properties);
                }

                fname = properties.filename;

                // GET request to the sync resource.
                WebRequest request = this.buildRequest(properties);

                // see if there are realm/userid/password preconditions
                WebConversation conversation = new WebConversation();
                this.setAuthentication(conversation, properties);

                process(conversation, request, properties);
            }

            printPropertiesFiles = false;
        } catch (Exception unexpected) {
            log.error("unexpected exception for " + fname, unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGET() {
        new SyncGetTest().testGet();
    }

    @Test
    public void testPOST() {
        new SyncPostTest().testPost();
    }

    private class SyncGetTest extends SyncTest {

        public SyncGetTest() {
            super();
        }

        public void testGet() {
            super.doTest();
        }

        protected WebRequest buildRequest(final TestProperties properties)
                throws UnsupportedEncodingException, MalformedURLException {
            // Build the request.
            StringBuilder sb = new StringBuilder();

            // Add parameters if available.
            if (properties.parameters != null) {
                Map<String, List<String>> parameters = properties.parameters;
                List<String> valueList;
                List<String> keyList = new ArrayList<String>(parameters.keySet());
                for (String key : keyList) {
                    valueList = parameters.get(key);
                    for (String value : valueList) {
                        sb.append(key);
                        sb.append("=");
                        sb.append(URLEncoder.encode(value, "UTF-8"));
                        sb.append("&");
                    }
                }
            }

            String getUrl = serviceUrl + "?" + sb.substring(0, sb.length() - 1); // strip trailing &
            WebRequest request = new GetMethodWebRequest(getUrl);
            log.debug("HTTP GET: " + request.getURL().toString());
            return request;
        }
    }

    private class SyncPostTest extends SyncTest {

        public SyncPostTest() {
            super();
        }

        public void testPost() {
            super.doTest();
        }

        protected WebRequest buildRequest(final TestProperties properties)
                throws UnsupportedEncodingException, MalformedURLException {
            // Build the request.            
            WebRequest request = new PostMethodWebRequest(serviceUrl);

            // Add parameters if available.
            if (properties.parameters != null) {
                Map<String, List<String>> parameters = properties.parameters;
                List<String> valueList;
                List<String> keyList = new ArrayList<String>(parameters.keySet());
                for (String key : keyList) {
                    valueList = parameters.get(key);
                    request.setParameter(key, valueList.toArray(new String[0]));
                }
            }

            log.debug("HTTP POST: " + request.getURL().toString());
            log.debug(Util.getRequestParameters(request));
            return request;
        }
    }
}
