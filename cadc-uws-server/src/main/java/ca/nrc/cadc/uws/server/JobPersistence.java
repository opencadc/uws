/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobRef;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Service interface for job persistence.
 */
public interface JobPersistence extends JobUpdater {
    /**
     * Shutdown and release any resources. This includes ThreadPools, connections, open files, etc.
     * @throws java.lang.InterruptedException
     */
    public void terminate()
        throws InterruptedException;

    /**
     * Obtain a Job from the persistence layer. Normally the job has
     * only the top-level job fields and the caller needs to call getDetails
     * if the parameters and results are needed.
     *
     * @param jobID
     * @return the job
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public Job get(String jobID)
        throws JobNotFoundException, JobPersistenceException, TransientException;

    /**
     * Get the details for the specified job. The details include all parameters
     * and results.
     *
     * @param job
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public void getDetails(Job job)
        throws JobPersistenceException, TransientException;

    /**
     * Persist the given Job. The returned job will have a jobID value.
     *
     * @param job
     * @return the persisted job
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public Job put(Job job)
        throws JobPersistenceException, TransientException;

    /**
     * Delete the specified job.
     *
     * @param jobID
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public void delete(String jobID)
        throws JobPersistenceException, TransientException;

    /**
     * Obtain a listing of JobRef instances.
     *
     * @param requestPath
     * @return iterator over visible jobs
     * @throws ca.nrc.cadc.uws.server.JobPersistenceException
     * @throws ca.nrc.cadc.net.TransientException
     */
    public Iterator<JobRef> iterator(String requestPath)
        throws JobPersistenceException, TransientException;

    /**
     * Obtain a listing of JobRef instances in the specified phase.
     *
     * @param requestPath
     * @param phases
     * @return iterator over visible jobs
     * @throws ca.nrc.cadc.uws.server.JobPersistenceException
     * @throws ca.nrc.cadc.net.TransientException
     */
    public Iterator<JobRef> iterator(String requestPath, List<ExecutionPhase> phases)
        throws JobPersistenceException, TransientException;

    /**
     * Obtain a listing of the last 'last' JobRef instances in the specified
     * phase with a start date after 'after'.
     *
     * @param requestPath
     * @param phases
     * @param after
     * @param last
     * @return iterator over visible jobs
     * @throws ca.nrc.cadc.uws.server.JobPersistenceException
     * @throws ca.nrc.cadc.net.TransientException
     */
    public Iterator<JobRef> iterator(String requestPath, List<ExecutionPhase> phases, Date after, Integer last)
        throws JobPersistenceException, TransientException;

}
