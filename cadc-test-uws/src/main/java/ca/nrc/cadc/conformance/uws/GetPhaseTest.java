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
import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * GET phase status should return only a short string, other than a long XML.
 *
 * @author zhangsa
 *
 */
public class GetPhaseTest extends AbstractUWSTest {

    private static Logger log = Logger.getLogger(GetPhaseTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }

    public GetPhaseTest() {
        super();
    }

    @Test
    public void testPhase() throws Throwable {
        try {
            // Create a new Job.
            WebConversation conversation = new WebConversation();
            String jobId = createJob(conversation);

            verifyPhase(conversation, jobId, "PENDING");

            // Delete the job.
            deleteJob(conversation, jobId);

            log.info("GetPhaseTest.testPhase completed.");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

    private void verifyPhase(WebConversation conversation, String jobId, String expectedPhase) throws Throwable {
        // GET request to the phase resource.
        String resourceUrl = serviceUrl + "/" + jobId + "/phase";
        WebResponse response = get(conversation, resourceUrl, "text/plain");

        log.debug(Util.getResponseHeaders(response));
        log.debug("Response.getText():\r\n" + response.getText());

        Assert.assertEquals("GET response Content-Type header to " + resourceUrl + " is incorrect",
                "text/plain", response.getContentType());

        Assert.assertEquals("response should return only a string of the phase.", expectedPhase,
                response.getText());
    }

    // canot have a run test without knowing job params; the ones here are for the example-uws 
    //@Test
    public void testRunPhase() throws Throwable {
        try {
            // Create a new Job.
            final WebConversation conversation = new WebConversation();
            Map<String, List<String>> parameters = new HashMap<String, List<String>>();
            List<String> pass = new ArrayList<String>();
            pass.add("FALSE");
            parameters.put("PASS", pass);
            List<String> runfor = new ArrayList<String>();
            runfor.add("10");
            parameters.put("RUNFOR", runfor);

            // Create a new Job and get the jobId.
            String jobId = createJob(conversation, parameters);

            // POST request to the phase resource.
            String resourceUrl = serviceUrl + "/" + jobId + "/phase";
            WebRequest postRequest = new PostMethodWebRequest(resourceUrl);
            postRequest.setParameter("PHASE", "RUN");
            post(conversation, postRequest);

            verifyPhase(conversation, jobId, "EXECUTING");

            // Delete the Job.
            deleteJob(conversation, jobId);

            log.info("GetPhaseTest.testRunPhase completed.");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

    @Test
    public void testAbortPhase() throws Throwable {
        try {
            // Create a new Job.
            WebConversation conversation = new WebConversation();
            String jobId = createJob(conversation);

            // POST request to the phase resource.
            String resourceUrl = serviceUrl + "/" + jobId + "/phase";
            WebRequest postRequest = new PostMethodWebRequest(resourceUrl);
            postRequest.setParameter("PHASE", "ABORT");
            post(conversation, postRequest);

            verifyPhase(conversation, jobId, "ABORTED");

            // Delete the Job.
            deleteJob(conversation, jobId);

            log.info("GetPhaseTest.testAbortPhase completed.");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

}
