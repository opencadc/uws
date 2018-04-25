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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobAttribute;
import ca.nrc.cadc.uws.JobWriter;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import org.apache.log4j.Logger;

/**
 * Get a job using jobID.
 *
 * @author pdowler
 */
public class GetAction extends JobAction {

    private static final Logger log = Logger.getLogger(GetAction.class);

    private static final long MAX_WAIT = 60L;
    private static final long POLL_INTERVAL[] = {1L, 2L, 4L, 8L};

    public GetAction() {
    }

    @Override
    public void doAction() throws Exception {
        String jobID = getJobID();
        String field = getJobField();
        log.debug("START: " + jobID + " " + field);
        if (jobID == null) {
            writeJobListing();
        }

        if (field == null) {
            Job job = doWait(jobID);
            writeJob(job);
        } else {
            Job job = jobManager.get(jobID);
            handleGetJobField(job, field);
        }
        log.debug("DONE: " + jobID + " " + field);
    }

    private void writeJobListing() throws IOException {
        throw new UnsupportedOperationException("get job listing");
    }

    private void writeJob(Job job) throws IOException {
        // TODO: content negotiation via accept header
        JobWriter w = new JobWriter();
        syncOutput.setHeader("Content-Type", "text/xml");
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);
        w.write(job, bc);
        logInfo.setBytes(bc.getByteCount());
    }

    private void handleGetJobField(Job job, String field) throws IOException {
        String value = null;
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        JobAttribute ja = JobAttribute.toValue(field);
        switch (ja) {
            case DESTRUCTION_TIME:
                value = df.format(job.getDestructionTime());
                break;
            case EXECUTION_DURATION:
                value = Long.toString(job.getExecutionDuration());
                break;
            case EXECUTION_PHASE:
                value = job.getExecutionPhase().getValue();
                break;
            case QUOTE:
                value = df.format(job.getQuote());
                break;
            case OWNER_ID:
                value = job.getOwnerID();
                break;
            default:
                throw new UnsupportedOperationException("not implemented: result listing");
        }

        syncOutput.setHeader("Content-Type", "text/plain");
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);
        PrintWriter w = new PrintWriter(bc);
        w.println(value);
        w.flush();
        logInfo.setBytes(bc.getByteCount());
    }

    private Job doWait(String jobID) throws JobNotFoundException, JobPersistenceException, TransientException {
        Job job = jobManager.get(jobID);
        String waitStr = syncInput.getParameter("WAIT");
        if (waitStr != null) {
            log.debug("wait = " + waitStr);
            try {
                long wait = Long.parseLong(waitStr);
                if (wait > MAX_WAIT) {
                    wait = MAX_WAIT;
                }
                if (wait < 0) {
                    wait = MAX_WAIT;
                }
                log.debug("wait: " + wait);
                ExecutionPhase ep = job.getExecutionPhase();
                if (ep.isActive()) {
                    ExecutionPhase cur = ep;
                    int n = 0;
                    long rem = 1000 * wait;
                    while (rem > 0 && ep.equals(cur)) {
                        long dt = 1000L * Math.min(POLL_INTERVAL[n], wait);
                        log.debug("wait: " + wait + " phase: " + ep.getValue() + " dt(ms): " + dt);
                        long t = Math.min(rem, dt);
                        log.debug("sleep: " + t);
                        try {
                            Thread.sleep(t);
                        } catch (InterruptedException ex) {
                            log.debug("interrupted: wait at phase " + ep.getValue());
                        }
                        job = jobManager.get(jobID); // always keep/return the latest job state
                        cur = job.getExecutionPhase();
                        rem -= dt;
                        if (n < POLL_INTERVAL.length - 1) {
                            n++;
                        }
                    }
                }
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("invalid WAIT value: " + waitStr);
            }
        }
        return job;
    }
}
