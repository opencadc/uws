/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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
************************************************************************
*/

package ca.nrc.cadc.uws.web;

import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobAttribute;
import ca.nrc.cadc.uws.server.JobManager;
import java.util.HashMap;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public abstract class JobAction extends RestAction {
    private static final Logger log = Logger.getLogger(JobAction.class);

    private String jobID;
    private String jobField;
    private String resultID;
    protected JobManager jobManager;
    
    // JobAttribute has strings used in XML document; the REST child resource names and update param names can differ
    protected static final Map<String,JobAttribute> CHILD_RESOURCE_NAMES = new HashMap<String,JobAttribute>();
    protected static final Map<JobAttribute,String> CHILD_PARAM_NAMES = new HashMap<JobAttribute,String>();
    
    static {
        CHILD_RESOURCE_NAMES.put("phase", JobAttribute.EXECUTION_PHASE);
        CHILD_RESOURCE_NAMES.put("executionduration", JobAttribute.EXECUTION_DURATION);
        CHILD_RESOURCE_NAMES.put("destruction", JobAttribute.DESTRUCTION_TIME);
        CHILD_RESOURCE_NAMES.put("quote", JobAttribute.QUOTE);
        CHILD_RESOURCE_NAMES.put("owner", JobAttribute.OWNER_ID); 
        CHILD_RESOURCE_NAMES.put("parameters", JobAttribute.PARAMETERS);
        
        CHILD_PARAM_NAMES.put(JobAttribute.EXECUTION_PHASE, JobAttribute.EXECUTION_PHASE.getValue().toUpperCase());
        CHILD_PARAM_NAMES.put(JobAttribute.EXECUTION_DURATION, JobAttribute.EXECUTION_DURATION.getValue().toUpperCase());
        CHILD_PARAM_NAMES.put(JobAttribute.DESTRUCTION_TIME, JobAttribute.DESTRUCTION_TIME.getValue().toUpperCase());
        CHILD_PARAM_NAMES.put(JobAttribute.QUOTE, JobAttribute.QUOTE.getValue().toUpperCase());
    }
    
    public JobAction() { 
        super();
    }
    
    /**
     * Initialisation that cannot be performed in the constructor. This method must
     * be called at the start of doAction in all subclasses.
     */
    protected void init() {
        if (jobManager == null) {
            String jndiKey = componentID + ".jobManager"; // see AsyncServlet
            try {
                Context ctx = new InitialContext();
                this.jobManager = (JobManager) ctx.lookup(jndiKey);
                if (jobManager != null) {
                    log.debug("found: " + jndiKey +"=" + jobManager.getClass().getName());
                } else {
                    log.error("BUG: failed to find " + jndiKey + " via JNDI");
                }
            } catch (NamingException ex) {
                log.error("BUG: failed to find " + jndiKey + " via JNDI", ex);
            }
        }
    }
    
    private void initTarget() {
        // [jobID[/jobField[/resultID]]]
        if (jobID == null) {
            String path = syncInput.getPath();
            if (path != null) {
                String[] parts = path.split("/");
                if (parts.length > 0) {
                    this.jobID = parts[0];
                    if (parts.length > 1) {
                        this.jobField = parts[1];
                        if (parts.length == 3) {
                            this.resultID = parts[2];
                        }
                    }
                }
            }
        }
        // TODO: validation
        log.debug("init: " + jobID + " " + jobField + " " + resultID);
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    protected String getJobListURL() {
        String ret = syncInput.getRequestURI();
        String path = syncInput.getPath();
        if (path != null) {
            ret = ret.replace("/" + path, ""); // syncInput removes leading /
        }
        return ret;
    }
    
    protected String getJobID() {
        initTarget();
        return jobID;
    }
    
    protected String getJobField() {
        initTarget();
        return jobField;
    }
    
    protected String getJobResultID() {
        initTarget();
        return resultID;
    }
}
