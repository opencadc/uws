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
************************************************************************
*/

package ca.nrc.cadc.uws.server.impl;

import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.uws.server.DatabaseJobPersistence;
import ca.nrc.cadc.uws.server.JobDAO.JobSchema;
import ca.nrc.cadc.uws.server.RandomStringGenerator;
import ca.nrc.cadc.uws.server.StringIDGenerator;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Basic JobPersistence implementation that uses the standard postgresql
 * database schema setup. See also InitDatabaseUWS to automate creating
 * and upgrading UWS tables.
 *
 * @author pdowler
 */
public class PostgresJobPersistence extends DatabaseJobPersistence {

    public static final String DEFAULT_DS_NAME = "jdbc/uws";
    
    private boolean storeOwnerASCII = false;

    public PostgresJobPersistence(IdentityManager im) {
        this(new RandomStringGenerator(16), im);
    }

    public PostgresJobPersistence(StringIDGenerator idg, IdentityManager im) {
        this(idg, im, false);
    }

    public PostgresJobPersistence(StringIDGenerator idg, IdentityManager im, boolean storeOwnerASCII) {
        super(idg, im);
        this.storeOwnerASCII = storeOwnerASCII;
    }

    /**
     * Find a JNDI DataSource with the default name (jdbc/uws) and return it.
     *
     * @return a JNDI DataSource
     * @throws NamingException
     */
    @Override
    protected DataSource getDataSource()
            throws NamingException {
        return DBUtil.findJNDIDataSource(DEFAULT_DS_NAME);
    }

    /**
     * The standard postgresql database configuration. Jobs are stored in a
     * table named uws.Jobs, parameters and results are stored in a
     * table named uws.JobDetail.
     *
     * @return
     */
    @Override
    protected JobSchema getJobSchema() {
        return new JobSchema("uws.Job", "uws.JobDetail", false, storeOwnerASCII);
    }

}
