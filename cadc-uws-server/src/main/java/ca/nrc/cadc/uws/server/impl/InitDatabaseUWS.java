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

import ca.nrc.cadc.db.version.InitDatabase;
import java.net.URL;
import java.util.Date;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * Optional class that automates creation and upgrading of UWS tables in a
 * database. This class currently only supports PostgreSQL. If you use this
 * class to initialise the UWS tables, you must also set JobSchema.storeOwnerASCII
 * when creating the JobSchema (e.g. via the PostgresJobPersistence).
 *
 * @author pdowler
 */
public class InitDatabaseUWS extends InitDatabase {
    private static final Logger log = Logger.getLogger(InitDatabaseUWS.class);

    public static final String MODEL_NAME = "UWS";
    public static final String MODEL_VERSION = "1.2.18";
    public static final String PREV_MODEL_VERSION = "1.2.16";

    public static final String[] CREATE_SQL = new String[]{
        "uws.ModelVersion.sql",
        "uws.Job.sql",
        "uws.JobDetail.sql",
        "uws.JobAvailability.sql",
        "uws.permissions.sql"
    };

    public static final String[] UPGRADE_SQL = new String[]{
        "uws.upgrade-1.2.18.sql"
    };

    public static final String[] MAINT_SQL = new String[]{
        "uws.rollover.sql",
        "uws.Job.sql",
        "uws.JobDetail.sql",
        "uws.permissions.sql"
    };

    public InitDatabaseUWS(DataSource dataSource, String database, String schema) {
        super(dataSource, database, schema, MODEL_NAME, MODEL_VERSION, PREV_MODEL_VERSION);
        for (String s : CREATE_SQL) {
            createSQL.add(s);
        }
        for (String s : UPGRADE_SQL) {
            upgradeSQL.add(s);
        }
        for (String s : MAINT_SQL) {
            maintenanceSQL.add(s);
        }
    }

    @Override
    public boolean doMaintenance(Date lastModified, String tag) {
        if (tag == null) {
            throw new IllegalArgumentException("null tag not allowed, must be valid part of table name");
        }
        return super.doMaintenance(lastModified, tag);
    }

    @Override
    protected URL findSQL(String fname) {
        // SQL files are stored inside the jar file
        return InitDatabaseUWS.class.getClassLoader().getResource("postgresql/" + fname);
    }
}
