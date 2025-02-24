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

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.version.InitDatabase;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.server.TestUtil;
import java.sql.SQLException;
import java.util.Date;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class InitDatabaseUWSTest {

    private static final Logger log = Logger.getLogger(InitDatabaseUWSTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.uws.server.impl", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
    }

    private DataSource dataSource;
    
    private static final String[] TABLE_NAMES = {
        TestUtil.SCHEMA + ".ModelVersion", 
        TestUtil.SCHEMA + ".JobAvailability",
        TestUtil.SCHEMA + ".JobDetail", // FK->Job
        TestUtil.SCHEMA + ".Job",
        TestUtil.SCHEMA + ".JobDetail_backup",
        TestUtil.SCHEMA + ".Job_backup"
    };

    public InitDatabaseUWSTest() {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            dataSource = DBUtil.getDataSource(cc, true, true);
        } catch (Exception ex) {
            log.error("failed to init DataSource", ex);
        }
    }

    @Before
    public void cleanup() throws Exception {
        for (String tn : TABLE_NAMES) {
            String sql = "DROP TABLE " + tn;
            log.info("cleanup: "  + sql);
            try {
                dataSource.getConnection().createStatement().execute(sql);
            } catch (SQLException oops) {
                log.warn("drop failed: " + oops);
            }
        }
    }
    
    @Test
    public void testNewInstall() {
        try {
            // TODO: nuke all tables and re-create
            // for now: create || upgrade || idempotent
            InitDatabase init = new InitDatabaseUWS(dataSource, TestUtil.DATABASE, TestUtil.SCHEMA);
            init.doInit();

            // TODO: verify that tables were created with test queries
            // TODO: verify that init is idempotent
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpgradeInstall() {
        try {
            // TODO: create previous version  of tables and upgrade... sounds complicated
            // for now: create || upgrade || idempotent
            InitDatabase init = new InitDatabaseUWS(dataSource, TestUtil.DATABASE, TestUtil.SCHEMA);
            init.doInit();

            // TODO: verify that tables were created with test queries
            // TODO: verify that init is idempotent
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testRollover() {
        try {
            InitDatabase init = new InitDatabaseUWS(dataSource, TestUtil.DATABASE, TestUtil.SCHEMA);
            init.doInit();

            Thread.sleep(100L);
            Date now = new Date(); // will proceed if tables are older than this
            log.info("doMaintenance: START");
            String tag = "backup";
            boolean maint = init.doMaintenance(now, tag);
            log.info("doMaintenance: " + maint);
            Assert.assertTrue(maint);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
