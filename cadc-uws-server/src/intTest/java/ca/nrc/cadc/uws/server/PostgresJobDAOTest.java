
package ca.nrc.cadc.uws.server;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;

/**
 *
 * @author pdowler
 */
public class PostgresJobDAOTest extends AbstractJobDAOTest {

    private static Logger log = Logger.getLogger(PostgresJobDAOTest.class);

    @BeforeClass
    public static void testSetup() {
        Log4jInit.setLevel("ca.nrc.cadc.uws.server", Level.INFO);
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig(TestUtil.SERVER, TestUtil.DATABASE);
            dataSource = DBUtil.getDataSource(cc);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());
            JOB_SCHEMA = new JobDAO.JobSchema(TestUtil.SCHEMA + ".Job", TestUtil.SCHEMA + ".JobDetail", false);
        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
    }
}
