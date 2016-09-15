/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.conformance.uws2;


import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.conformance.uws.TestProperties;
import ca.nrc.cadc.conformance.uws.TestPropertiesList;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import static org.junit.Assert.fail;

/**
 *
 * @author pdowler
 */
public abstract class AbstractUWSTest2 
{
    private static final Logger log = Logger.getLogger(AbstractUWSTest2.class);

    protected final URI resourceID;
    protected final URI standardID;
    protected URL jobListURL;
    
    // track these to help debug test config
    protected File propertiesDir;
    protected TestPropertiesList testPropertiesList;
    
    protected String testFilePrefix;
    
    public AbstractUWSTest2(URI resourceID, URI standardID)
    {
        this.resourceID = resourceID;
        this.standardID = standardID;
        
        RegistryClient rc = new RegistryClient();
        this.jobListURL = rc.getServiceURL(resourceID, standardID, AuthMethod.ANON);
        if (jobListURL == null)
            throw new RuntimeException("init FAIL, service not found:" + resourceID + " " + standardID + " " + AuthMethod.ANON);
        log.info("jobListURL: " + jobListURL);
    }
    
    /**
     * Set properties directory and load all matching properties files.
     * 
     * @param propertiesDir
     * @param testFilePrefix 
     */
    protected void setPropertiesDir(File propertiesDir, String testFilePrefix)
    {
        if (propertiesDir == null)
        {
            String pdir = System.getProperty("properties.directory");
            if (pdir != null)
                propertiesDir = new File(pdir);
        }
        
        if (propertiesDir == null)
            Assert.fail("test config fail: propertiesDir not specified");
        
        if (testFilePrefix == null) // the actual test class that is running
            testFilePrefix = this.getClass().getSimpleName();
        
        this.testFilePrefix = testFilePrefix;
        this.propertiesDir = propertiesDir;
        
        try
        {
            testPropertiesList = new TestPropertiesList(propertiesDir.getPath(), testFilePrefix);
            log.info(testFilePrefix + ": found " + testPropertiesList.propertiesList.size() + " tests in " + propertiesDir);
            if (testPropertiesList.propertiesList.isEmpty())
                fail(testFilePrefix + ": no matching properties file(s) in " + propertiesDir);
        }
        catch (IOException e)
        {
            log.error(e);
            fail(e.getMessage());
        }
    }
    
    /**
     * Create and execute a synchronous job specified by parameters.
     * 
     * @param jobName name to be logged
     * @param params parameters for this job
     * @return 
     */
    protected JobResultWrapper createAndExecuteSyncParamJobPOST(String jobName, Map<String,Object> params)
    {
        JobResultWrapper ret = new JobResultWrapper(jobName);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            log.info(jobName + ": POST " + jobListURL);
            HttpPost doit = new HttpPost(jobListURL, params, bos);
            doit.run();

            ret.job = null; // no formal way to get jobID
            ret.throwable = doit.getThrowable();
            ret.responseCode = doit.getResponseCode();
            ret.contentType = doit.getResponseContentType();
            ret.contentEncoding = doit.getResponseContentEncoding();
            ret.syncOutput = bos.toByteArray();
        }
        finally { }
        
        return ret;
    }
    
    /**
     * Create and execute a synchronous job specified by parameters.
     * 
     * @param jobName name to be logged
     * @param params parameters for this job
     * @return 
     */
    protected JobResultWrapper createAndExecuteSyncParamJobGET(String jobName, Map<String,Object> params)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(jobListURL.toExternalForm()).append("?");
        for (Map.Entry<String,Object> me : params.entrySet())
        {
            if (me.getValue() != null)
            {
                if (me.getValue() instanceof Collection)
                {
                    Collection col = (Collection) me.getValue();
                    for (Object v : col)
                    {
                        sb.append(me.getKey()).append("=").append(NetUtil.encode(v.toString()));
                        sb.append("&");
                    }
                }
                else
                {
                    sb.append(me.getKey()).append("=").append(NetUtil.encode(me.getValue().toString()));
                    sb.append("&");
                }
            }
        }
        String surl = sb.toString();
        
        JobResultWrapper ret = new JobResultWrapper(jobName);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            URL getURL = new URL(surl);

            log.info(jobName + ": GET " + getURL);
            HttpDownload doit = new HttpDownload(getURL, bos);
            doit.run();
            
            ret.job = null; // no formal way to get jobID
            ret.throwable = doit.getThrowable();
            ret.responseCode = doit.getResponseCode();
            ret.contentType = doit.getContentType();
            ret.contentEncoding = doit.getContentEncoding();
            ret.syncOutput = bos.toByteArray();
        }
        catch(MalformedURLException ex)
        {
            Assert.fail(jobName + ": failed to generate valid GET URL: " + surl + " reason: " + ex);
        }
        
        return ret;
    }
    
    protected URL createAsyncParamJob(String jobName, Map<String,Object> params)
    {
        HttpPost post = new HttpPost(jobListURL, params, false);
        post.run();
        if (post.getThrowable() != null)
            Assert.fail("failed to create job: " + jobName + " reason: " + post.getThrowable());
        
        URL ret = post.getRedirectURL();
        Assert.assertNotNull("redirectURL", ret);
        log.info(jobName + ": created " + ret);
        return ret;
    }
    
    protected URL createAsyncDocumentJob(String document, String contentType)
    {
        throw new UnsupportedOperationException("not implemented");
    }
    
    protected Job executeAsyncJob(URL jobURL, long timeout, long pollInterval)
    {
        Map<String,Object> params = new TreeMap<String,Object>();
        params.put("PHASE", "RUN");
        HttpPost post = new HttpPost(jobURL, params, false);
        post.run();
        if (post.getThrowable() != null)
            Assert.fail("failed to set PHASE=RUN for " + jobURL + " reason: " + post.getThrowable());
        
        long start = System.currentTimeMillis();
        boolean done = false;
        
        try
        {
            JobReader r = new JobReader();
            while (!done && (start + timeout) < System.currentTimeMillis())
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                HttpDownload get = new HttpDownload(jobURL, bos);
                get.run();
                if (get.getThrowable() != null)
                    Assert.fail("failed to check phase for " + jobURL + " reason: " + get.getThrowable());

                Job j = r.read(new ByteArrayInputStream(bos.toByteArray()));
                if ( j.getExecutionPhase().isActive() )
                {
                    Thread.sleep(pollInterval);
                }
                else
                    return j;
            }
        }
        catch(InterruptedException ex)
        {
            Assert.fail("failed to wait for job " + jobURL + " reason: " + ex);
        }
        catch(Exception ex)
        {
            Assert.fail("failed to read job " + jobURL + " reason: " + ex);
        }
        
        throw new RuntimeException("timeout: job " + jobURL + " did not complete after " + timeout + "ms");
    }
    
    protected String getExpectedContentType(final TestProperties properties)
    {
        String contentType = null;
        if (properties.expectations != null)
        {
            if (properties.expectations.containsKey("Content-Type"))
            {
                contentType = properties.expectations.get("Content-Type").get(0);
            }            
        }
        
        return contentType;
    }
    
    protected Integer getExpectedResponseCode(final TestProperties properties)
    {
        Integer responseCode = null; 
        if (properties.expectations != null)
        {
            if (properties.expectations.containsKey("Response-Code"))
            {
                responseCode = Integer.valueOf(properties.expectations.get("Response-Code").get(0));
            }
        }
        
        return responseCode;
    }
    
    protected ExecutionPhase getExpectedPhase(final TestProperties properties)
    {
        ExecutionPhase ret = null;
        if (properties.expectations != null)
        {
            if (properties.expectations.containsKey("Execution-Phase"))
            {
                String s = properties.expectations.get("Execution-Phase").get(0);
                ret = ExecutionPhase.toValue(s);
            }            
        }
        
        return ret;
    }
}
