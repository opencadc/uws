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


import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobAttribute;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobPhaseException;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class PostAction extends JobAction {
    private static final Logger log = Logger.getLogger(PostAction.class);

    public PostAction() { 
    }

    @Override
    public void doAction() throws Exception {
        String jobID = getJobID();
        String field = getJobField();
        boolean redirect = true;
        
        if (jobID == null) {
            // create
            JobCreator jc = new JobCreator();
            Job in = jc.create(syncInput);
            Job job = jobManager.create(in);
            jobID = job.getID();
            redirect = true;
        } else if (field == null && isDeleteAction()) {
            jobManager.delete(jobID);
        } else if (field != null) {
            // uws job control
            JobAttribute ja = JobAttribute.toValue(field);
            switch (ja) {
                case EXECUTION_PHASE:
                    // start or stop job with PHASE=RUN|ABORT
                    doPhaseChange(jobID);
                    break;
                //case DESTRUCTION_TIME:
                //    break;
                //case EXECUTION_DURATION:
                //    break;
                //case QUOTE:
                //    break;
                default:
                    throw new UnsupportedOperationException("not implemented: UWS job control");
            }
        } else {
            // new job params
            JobCreator jc = new JobCreator();
            Job tmp = jc.create(syncInput);
            jobManager.update(jobID, tmp.getParameterList());
        }
        
        if (redirect) {
            String jobURL = syncInput.getRequestURI() + "/" + jobID;
            log.debug("redirect: " + jobURL);
            syncOutput.setHeader("Location", jobURL);
            syncOutput.setCode(303);
        } else {
            // TODO: remove this if never needed?
            syncOutput.setCode(200);
        }
    }
    
    private void doPhaseChange(String jobID) throws JobNotFoundException, JobPhaseException, JobPersistenceException, TransientException {
        String nep = syncInput.getParameter("PHASE");
        boolean run = "RUN".equalsIgnoreCase(nep);
        boolean abort = "ABORT".equalsIgnoreCase(nep);
        // check
        Job job = jobManager.get(jobID);
        ExecutionPhase ep = job.getExecutionPhase();
        if (abort) {
            jobManager.abort(jobID);
        } else if (run) {
            jobManager.execute(jobID);
        } else {
            throw new IllegalArgumentException("invalid PHASE value: " + nep);
        }
    }
    
    private boolean isDeleteAction() {
        String action = syncInput.getParameter("ACTION");
        if (action != null && "DELETE".equalsIgnoreCase(action)) {
            return true;
        }
        return false;
    }
}
