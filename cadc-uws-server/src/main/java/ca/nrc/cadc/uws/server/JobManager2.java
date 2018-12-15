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

package ca.nrc.cadc.uws.server;

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobRef;
import ca.nrc.cadc.uws.Parameter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * New JobManager API that supports job persistence and execution routing based on request path.
 * 
 * @author pdowler
 */
public interface JobManager2 {

    void terminate() throws InterruptedException;
    
    Job create(String requestPath, Job job) 
        throws JobPersistenceException, TransientException;

    void delete(String requestPath, String jobID) 
        throws JobNotFoundException, JobPersistenceException, TransientException;

    Job get(String requestPath, String jobID) 
        throws JobNotFoundException, JobPersistenceException, TransientException;

    Iterator<JobRef> iterator(String requestPath, String appname) 
        throws JobPersistenceException, TransientException;

    Iterator<JobRef> iterator(String requestPath, String appname, List<ExecutionPhase> phases) 
        throws JobPersistenceException, TransientException;

    Iterator<JobRef> iterator(String requestPath, String appname, List<ExecutionPhase> phases, Date after, Integer last) 
        throws JobPersistenceException, TransientException;
    
    void update(String requestPath, String jobID, Date destruction, Long duration, Date quote) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;

    void update(String requestPath, String jobID, List<Parameter> params) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;

    /**
     * Max elapsed time from job creation to destruction.
     *
     * @param maxDestruction
     */
    void setMaxDestruction(Long maxDestruction);

    /**
     * Max elapsed time a job will be allowed to execute.
     *
     * @param maxExecDuration
     */
    void setMaxExecDuration(Long maxExecDuration);

    /**
     * Max elapsed time until job should be finished.
     *
     * @param maxQuote
     */
    void setMaxQuote(Long maxQuote);

    void execute(String requestPath, String jobID) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;

    void execute(String requestPath, Job job) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;

    void execute(String requestPath, String jobID, SyncOutput output) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;

    void execute(String requestPath, Job job, SyncOutput output) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;
    
    void abort(String requestPath, String jobID) 
        throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException;
}
