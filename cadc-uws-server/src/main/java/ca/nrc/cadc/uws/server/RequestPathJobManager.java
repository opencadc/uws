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

import java.util.Map;
import java.util.TreeMap;
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
public class RequestPathJobManager extends SimpleJobManager {

    private static final Logger log = Logger.getLogger(RequestPathJobManager.class);

    protected final Map<String,JobPersistence> jobPersistenceMap = new TreeMap<>();
    protected final Map<String,JobExecutor> jobExecutorMap = new TreeMap<>();

    public RequestPathJobManager() {
        super();
    }

    @Override
    public void terminate()
            throws InterruptedException {
        super.terminate();
        for (JobPersistence jp : jobPersistenceMap.values()) {
            jp.terminate();
        }
        for (JobExecutor je : jobExecutorMap.values()) {
            je.terminate();
        }
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
     * @param jobUpdater
     * @return a JobExecutor instance
     */
    protected JobExecutor createJobExecutor(String requestPath, JobUpdater jobUpdater) {
        return null;
    }
    
    @Override
    protected JobPersistence getJobPersistence(String requestPath) {
        JobPersistence ret = super.getJobPersistence(requestPath);
        if (ret == null) {
            ret = jobPersistenceMap.get(requestPath);
            if (ret == null) {
                ret = createJobPersistence(requestPath);
                jobPersistenceMap.put(requestPath, ret);
            }
        }
        return ret;
    }
    
    @Override
    protected JobExecutor getJobExecutor(String requestPath, JobUpdater jobUpdater) {
        JobExecutor ret = super.getJobExecutor(requestPath, jobUpdater);
        if (ret == null) {
            ret = jobExecutorMap.get(requestPath);
            if (ret == null) {
                ret = createJobExecutor(requestPath, jobUpdater);
                jobExecutorMap.put(requestPath, ret);
            }
        }
        return ret;
    }
}
