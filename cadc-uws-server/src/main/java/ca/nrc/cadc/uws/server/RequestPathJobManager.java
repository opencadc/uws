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
*  $Revision: 4 $
*
************************************************************************
 */

package ca.nrc.cadc.uws.server;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobRef;
import ca.nrc.cadc.uws.Parameter;
import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 * Implementation of the JobManager interface that can use the request path to instantiate
 * and invoke different JobPersistence and JobExecutor instances. The intended usage is
 * to subclass this and either (i) call setJobPersistence and/or setJobExecutor to use single
 * instances (that ignore request path) in the constructor, or (ii) override createJobPersistence
 * and/or createJobExecutor to instantiate instances for use with different paths.
 *
 * @author pdowler
 */
public class RequestPathJobManager implements JobManager2 {

    private static final Logger log = Logger.getLogger(RequestPathJobManager.class);

    protected JobPersistence jobPersistence;
    protected Map<String,JobPersistence> jobPersistenceMap = new TreeMap<>();
    
    protected JobExecutor jobExecutor;
    protected Map<String,JobExecutor> jobExecutorMap = new TreeMap<>();
    protected Long maxExecDuration = 3600L;
    protected Long maxQuote = 3600L;
    protected Long maxDestruction = 7 * 24 * 3600L;

    public RequestPathJobManager() {
    }

    @Override
    public void terminate()
            throws InterruptedException {
        for (JobPersistence jp : jobPersistenceMap.values()) {
            jp.terminate();
        }
        for (JobExecutor je : jobExecutorMap.values()) {
            je.terminate();
        }
    }

    /**
     * Set a single (global) JobPersistence instance to be used for all requests.
     * This supports backwards-compatible behaviour of using this instance and
     * ignoring the request path. If this method is not called or is called with a
     * null argument, createJobPersistence(requestpath) will be used to create
     * instances as necessary.
     * 
     * @param jobPersistence 
     */
    public void setJobPersistence(JobPersistence jobPersistence) {
        this.jobPersistence = jobPersistence;
    }

    /**
     * Set a single (global) JobExecutor instance to be used for all requests.
     * This supports backwards-compatible behaviour of using this instance and
     * ignoring the request path. If this method is not called or is called with a
     * null argument, createJobExecutor(requestpath) will be used to create
     * instances as necessary.
     * 
     * @param jobExecutor 
     */
    public void setJobExecutor(JobExecutor jobExecutor) {
        this.jobExecutor = jobExecutor;
    }
    
    /**
     * Create a JobPersistence instance for the specified request path.  The same
     * instance (e.g. a PostgresJobPersistence) can be returned if you want to use a 
     * single instance (e.g. single connection pool and single database) for one 
     * or more sub-paths. 
     * 
     * @param requestPath
     * @return a JobPersistence instance
     */
    protected JobPersistence createJobPersistence(String requestPath) { 
        return null;
    }
    
    /**
     * Create a JobExecutor instance for the specified request path. The same
     * instance (e.g. a ThreadPoolExecutor) if you want to use a single instance
     * for one or more sub-paths.
     * 
     * @param requestPath
     * @return a JobExecutor instance
     */
    protected JobExecutor createJobExecutor(String requestPath) {
        return null;
    }
    
    private JobPersistence getJobPersistence(String requestPath) {
        if (jobPersistence != null) { 
            return jobPersistence;
        }
        
        JobPersistence ret = jobPersistenceMap.get(requestPath);
        if (ret == null) {
            ret = createJobPersistence(requestPath);
            jobPersistenceMap.put(requestPath, ret);
        }
        return ret;
    }
    
    private JobExecutor getJobExecutor(String requestPath) {
        if (jobExecutor != null) {
            return jobExecutor;
        }
        
        JobExecutor ret = jobExecutorMap.get(requestPath);
        if (ret == null) {
            ret = createJobExecutor(requestPath);
            jobExecutorMap.put(requestPath, ret);
        }
        return ret;
    }

    /**
     * Max elapsed time a job will be allowed to execute.
     *
     * @param maxExecDuration
     */
    @Override
    public void setMaxExecDuration(Long maxExecDuration) {
        this.maxExecDuration = maxExecDuration;
    }

    /**
     * Max elapsed time from job creation to destruction.
     *
     * @param maxDestruction
     */
    @Override
    public void setMaxDestruction(Long maxDestruction) {
        this.maxDestruction = maxDestruction;
    }

    /**
     * Max elapsed time until job should be finished.
     *
     * @param maxQuote
     */
    @Override
    public void setMaxQuote(Long maxQuote) {
        this.maxQuote = maxQuote;
    }

    @Override
    public void abort(String requestPath, String jobID)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        JobExecutor jobExecutor = getJobExecutor(requestPath);
        jobExecutor.abort(job);
    }

    @Override
    public Job create(String requestPath, Job job)
            throws JobPersistenceException, TransientException {
        // set defaults
        job.setExecutionPhase(ExecutionPhase.PENDING);
        JobPersistenceUtil.constrainDestruction(job, 1, maxDestruction);
        JobPersistenceUtil.constrainDuration(job, 1, maxExecDuration);
        JobPersistenceUtil.constrainQuote(job, 1, maxQuote);
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        return jobPersistence.put(job);
    }

    @Override
    public void delete(String requestPath, String jobID)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        jobPersistence.delete(jobID);
    }

    @Override
    public void execute(String requestPath, String jobID)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        execute(requestPath, job);
    }

    @Override
    public void execute(String requestPath, Job job)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        // assume they used get which does auth check and getDetails
        JobExecutor jobExecutor = getJobExecutor(requestPath);
        jobExecutor.execute(job);
    }

    @Override
    public void execute(String requestPath, String jobID, SyncOutput output)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        jobPersistence.getDetails(job);
        execute(requestPath, job, output);
    }

    @Override
    public void execute(String requestPath, Job job, SyncOutput output)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        JobExecutor jobExecutor = getJobExecutor(requestPath);
        jobExecutor.execute(job, output);
    }

    @Override
    public Job get(String requestPath, String jobID)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        jobPersistence.getDetails(job);
        return job;
    }

    @Override
    public Iterator<JobRef> iterator(String requestPath, String appname)
            throws JobPersistenceException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        return jobPersistence.iterator(appname);
    }

    @Override
    public Iterator<JobRef> iterator(String requestPath, String appname, List<ExecutionPhase> phases)
            throws JobPersistenceException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        return jobPersistence.iterator(appname, phases);
    }

    @Override
    public Iterator<JobRef> iterator(String requestPath, String appname, List<ExecutionPhase> phases, Date after, Integer last)
            throws JobPersistenceException, TransientException {
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        return jobPersistence.iterator(appname, phases, after, last);
    }

    @Override
    public void update(String requestPath, String jobID, Date destruction, Long duration, Date quote)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        log.debug("update: " + jobID + "," + destruction + "," + duration + "," + quote);
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        if (!ExecutionPhase.PENDING.equals(job.getExecutionPhase())) {
            throw new JobPhaseException("cannot update job control details when phase=" + job.getExecutionPhase());
        }

        if (destruction != null) {
            job.setDestructionTime(destruction);
        }
        if (duration != null) {
            job.setExecutionDuration(duration);
        }
        if (quote != null) {
            job.setQuote(quote);
        }

        JobPersistenceUtil.constrainDestruction(job, 1, maxDestruction);
        JobPersistenceUtil.constrainDuration(job, 1, maxExecDuration);
        JobPersistenceUtil.constrainQuote(job, 1, maxQuote);

        jobPersistence.put(job);
    }

    @Override
    public void update(String requestPath, String jobID, List<Parameter> params)
            throws JobNotFoundException, JobPersistenceException, JobPhaseException, TransientException {
        log.debug("update: " + jobID + "," + toString(params));
        if (params == null || params.size() == 0) {
            return;
        }
        JobPersistence jobPersistence = getJobPersistence(requestPath);
        Job job = jobPersistence.get(jobID);
        doAuthorizationCheck(job);
        if (!ExecutionPhase.PENDING.equals(job.getExecutionPhase())) {
            throw new JobPhaseException("cannot update job control details when phase=" + job.getExecutionPhase());
        }
        jobPersistence.addParameters(jobID, params);
    }

    private String toString(List list) {
        if (list == null) {
            return "null";
        }
        return "List[" + list.size() + "]";
    }

    /**
     * Checks that the current caller is equivalent to the job owner.
     *
     * @param job The Job to check authorization to.
     * @throws AccessControlException If the current subject is not
     * authorized.
     */
    protected void doAuthorizationCheck(Job job)
            throws AccessControlException {
        log.debug("doAuthorizationCheck: " + job.getID() + "," + job.getOwnerID());
        if (job.ownerSubject == null) {
            return;
        }
        AccessControlContext acContext = AccessController.getContext();
        Subject caller = Subject.getSubject(acContext);
        if (caller != null) {
            Set<Principal> ownerPrincipals = job.ownerSubject.getPrincipals();
            Set<Principal> callerPrincipals = caller.getPrincipals();
            for (Principal oPrin : ownerPrincipals) {
                for (Principal cPrin : callerPrincipals) {
                    log.debug("doAuthorizationCheck: " + oPrin + " vs " + cPrin);
                    if (AuthenticationUtil.equals(oPrin, cPrin)) {
                        return; // caller===owner
                    }
                }
            }
        }
        throw new AccessControlException("permission denied");
    }

}
