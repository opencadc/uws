
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
 * database schema setup. 
 * @author pdowler
 */
public class PostgresJobPersistence extends DatabaseJobPersistence
{
    public static final String DEFAULT_DS_NAME = "jdbc/uws";
    
    public PostgresJobPersistence(IdentityManager im)
    {
        this(new RandomStringGenerator(16), im);
    }
    
    public PostgresJobPersistence(StringIDGenerator idg, IdentityManager im)
    {
        super(idg, im);
    }

    /**
     * Find a JNDI DataSource with the default name (jdbc/uws) and return it.
     * 
     * @return a JNDI DataSource
     * @throws NamingException 
     */
    @Override
    protected DataSource getDataSource()
        throws NamingException
    {
        return DBUtil.findJNDIDataSource(DEFAULT_DS_NAME);
    }

    /**
     * The standard postgresql database configuration. Jobs are stored in  a 
     * table named uws.Jobs, params and results are stored in a 
     * table named uws.JobDetail.
     * 
     * @return 
     */
    @Override
    protected JobSchema getJobSchema()
    {
        return new JobSchema("uws.Job", "uws.JobDetail", false);
    }
    
    
}
