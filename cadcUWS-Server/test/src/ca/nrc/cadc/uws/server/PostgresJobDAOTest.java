
package ca.nrc.cadc.uws.server;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;

/**
 *
 * @author pdowler
 */
public class PostgresJobDAOTest extends AbstractJobDAOTest
{
    private static Logger log = Logger.getLogger(AbstractJobDAOTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.uws.server", Level.INFO);
        try
        {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("UWS_PG_TEST", "cadctest");
            dataSource = DBUtil.getDataSource(cc);
            log.info("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());

            String userName = System.getProperty("user.name");
            JOB_SCHEMA = new JobDAO.JobSchema(userName + ".Job", userName + ".JobDetail", false);
        }
        catch(Exception ex)
        {
            log.error("setup failed", ex);
            throw new IllegalStateException("failed to create DataSource", ex);
        }
    }
}
