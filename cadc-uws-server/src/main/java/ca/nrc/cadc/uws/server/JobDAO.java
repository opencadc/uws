/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.JobRef;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * JobDAO class that stores the jobs in a RDBMS.
 *
 * @author pdowler, jburke
 */
public class JobDAO {

    private static Logger log = Logger.getLogger(JobDAO.class);

    private static final int BATCH_SIZE = 1000;
    private static final String TYPE_PARAMETER = "P";
    private static final String TYPE_RESULT = "R";

    private static String[] JOB_COLUMNS
            = {
                // jobID must be first
                // creationTime must be last

                "jobID", "creationTime", "executionPhase", "executionDuration", "destructionTime",
                "quote", "startTime", "endTime",
                "error_summaryMessage", "error_type", "error_documentURL",
                "ownerID", "runID", "requestPath", "remoteIP",
                "jobInfo_content", "jobInfo_contentType", "jobInfo_valid",
                "lastModified"
            };
    private static String[] DETAIL_COLUMNS
            = {
                "jobID", "type", "name", "value"
            };

    private JobSchema jobSchema;
    private IdentityManager identManager;
    private StringIDGenerator idGenerator;

    private JdbcTemplate jdbc;
    private DataSourceTransactionManager transactionManager;
    private DefaultTransactionDefinition defaultTransactionDef;
    private TransactionStatus transactionStatus;
    private boolean inTransaction = false;

    // ugh: mix of formatting debug and SQL values
    private DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private DateFormat isoDateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    private Calendar cal = Calendar.getInstance(DateUtil.UTC);

    private Profiler prof = new Profiler(JobDAO.class);

    /**
     * Simple class that describes the database implementation. The caller can
     * specify the names for the job and job details tables.
     */
    public static class JobSchema {

        public String jobTable;
        public String detailTable;
        boolean limitWithTop;
        boolean storeOwnerASCII = false;

        /*
         * Map of columns that have size limits in the jobTable imposed by the
         * underlying database system.
         *
         * If a value destined for a column is longer than the limit, the
         * JobDAO will use an alternate column with <code>_text</code> appended to
         * the original column name. For example, the jobInfo_content column could
         * contain an arbitrary-length string. One could make this column type
         * VARCHAR(1024) to store small values, set the limit to 1024, and then
         * have an extra column named jobInfo_content_text of type TEXT for longer
         * values.
         */
        public Map<String, Integer> jobColumnLimits;

        /*
         * Map of columns that have size limits in the detailTable imposed by the
         * underlying database system.
         *
         * If a value destined for a column is longer than the limit, the
         * JobDAO will use an alternate column with <code>_text</code> appended to
         * the original column name. For example, the jobInfo_content column could
         * contain an arbitrary-length string. One could make this column type
         * VARCHAR(1024) to store small values, set the limit to 1024, and then
         * have an extra column named jobInfo_content_text of type TEXT for longer
         * values.
         */
        public Map<String, Integer> detailColumnLimits;

        // Map of alternate columns when the size limit in <code>columnLimits</code>
        private Map<String, String> alternateJobColumns;
        private Map<String, String> alternateDetailColumns;
        private List<String> jobColumns;
        private List<String> detailColumns;

        public JobSchema(String jobTable, String detailTable, boolean limitWithTop) {
            this(jobTable, detailTable, limitWithTop, null, null);
        }

        public JobSchema(String jobTable, String detailTable, boolean limitWithTop, boolean storeOwnerASCII) {
            this(jobTable, detailTable, limitWithTop, storeOwnerASCII, null, null);
        }

        public JobSchema(String jobTable, String detailTable, boolean limitWithTop,
                Map<String, Integer> jobColumnLimits, Map<String, Integer> detailColumnLimits) {
            this(jobTable, detailTable, limitWithTop, false, jobColumnLimits, detailColumnLimits);
        }

        public JobSchema(String jobTable, String detailTable, boolean limitWithTop, boolean storeOwnerASCII,
                Map<String, Integer> jobColumnLimits, Map<String, Integer> detailColumnLimits) {
            this.jobTable = jobTable;
            this.detailTable = detailTable;
            this.limitWithTop = limitWithTop;
            this.storeOwnerASCII = storeOwnerASCII;
            this.jobColumnLimits = jobColumnLimits;
            this.detailColumnLimits = detailColumnLimits;

            if (this.jobColumnLimits == null) {
                this.jobColumnLimits = new HashMap<String, Integer>();
            }
            this.alternateJobColumns = new HashMap<String, String>();
            for (String col : this.jobColumnLimits.keySet()) {
                alternateJobColumns.put(col, col + "_text");
            }

            if (this.detailColumnLimits == null) {
                this.detailColumnLimits = new HashMap<String, Integer>();
            }
            this.alternateDetailColumns = new HashMap<String, String>();
            for (String col : this.detailColumnLimits.keySet()) {
                alternateDetailColumns.put(col, col + "_text");
            }

            this.jobColumns = new ArrayList<String>();
            for (String col : JOB_COLUMNS) {
                jobColumns.add(col);
                String alt = alternateJobColumns.get(col);
                if (alt != null) {
                    jobColumns.add(alt);
                }
            }
            this.detailColumns = new ArrayList<String>();
            for (String col : DETAIL_COLUMNS) {
                detailColumns.add(col);
                String alt = alternateDetailColumns.get(col);
                if (alt != null) {
                    detailColumns.add(alt);
                }
            }
        }

        @Override
        public String toString() {
            return jobTable + "/" + detailTable + "/" + jobColumnLimits.size();
        }

        /**
         * Get the physical column length in the database. The default implementation always
         * returns null (no limit).
         *
         * @param tableName
         * @param columnName
         * @return max string size that can be stored, or null for no limit
         */
        public Integer getColumnLength(String tableName, String columnName) {
            if (jobColumnLimits != null && jobTable.equals(tableName)) {
                return jobColumnLimits.get(columnName);
            }
            if (detailColumnLimits != null && detailTable.equals(tableName)) {
                return detailColumnLimits.get(columnName);
            }
            return null; // no limit
        }

        public String getAlternateColumn(String tableName, String columnName) {
            String ret = null;
            if (alternateJobColumns != null && jobTable.equals(tableName)) {
                ret = alternateJobColumns.get(columnName);
            }
            if (alternateDetailColumns != null && detailTable.equals(tableName)) {
                ret = alternateDetailColumns.get(columnName);
            }
            return ret;
        }

    }

    public JobDAO(DataSource dataSource, JobSchema jobSchema, IdentityManager identManager, StringIDGenerator idGenerator) {
        this.jobSchema = jobSchema;
        this.identManager = identManager;
        this.idGenerator = idGenerator;

        this.defaultTransactionDef = new DefaultTransactionDefinition();
        defaultTransactionDef.setIsolationLevel(DefaultTransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.transactionManager = new DataSourceTransactionManager(dataSource);

        this.jdbc = new JdbcTemplate(dataSource);
    }

    /**
     * Start a transaction to the data source.
     */
    protected void startTransaction() {
        if (transactionStatus != null) {
            throw new IllegalStateException("transaction already in progress");
        }
        log.debug("startTransaction");
        this.transactionStatus = transactionManager.getTransaction(defaultTransactionDef);
        this.inTransaction = true;
        log.debug("startTransaction: OK");
    }

    /**
     * Commit the transaction to the data source.
     */
    protected void commitTransaction() {
        if (transactionStatus == null) {
            throw new IllegalStateException("no transaction in progress");
        }
        log.debug("commitTransaction");
        transactionManager.commit(transactionStatus);
        this.transactionStatus = null;
        this.inTransaction = false;
        log.debug("commit: OK");
    }

    /**
     * Rollback the transaction to the data source.
     */
    protected void rollbackTransaction() {
        if (!inTransaction) {
            throw new IllegalStateException("no transaction in progress");
        }
        if (transactionStatus == null) {
            return;
        }
        log.debug("rollbackTransaction");
        transactionManager.rollback(transactionStatus);
        this.transactionStatus = null;
        this.inTransaction = false;
        log.debug("rollback: OK");
    }

    /**
     * Obtain a Job from the persistence layer.
     *
     * @param jobID
     * @return the job
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public Job get(String jobID)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("get: " + jobID);
        try {
            JobSelectStatementCreator sc = new JobSelectStatementCreator();
            sc.setJobID(jobID);
            Job ret = (Job) jdbc.query(sc, new JobExtractor(jobSchema));
            prof.checkpoint("JobSelectStatementCreator");
            if (ret != null) {
                // call IdentityManager outside the resource lock to avoid deadlock
                if (ret.ownerID != null) {
                    ret.owner = identManager.toSubject(ret.ownerID);
                    ret.ownerID = identManager.toOwner(ret.owner); // type change
                    ret.ownerDisplay = identManager.toDisplayString(ret.owner);
                    prof.checkpoint("IdentityManager.toSubject");
                }
                return ret;
            }

        } catch (Throwable t) {
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to get job: " + jobID, t);
            } else {
                throw new JobPersistenceException("failed to get job: " + jobID, t);
            }
        }
        throw new JobNotFoundException(jobID);
    }

    /**
     * Load all stored Parameter(s) and Result(s) into the specified job.
     *
     * @param job
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public void getDetails(Job job)
            throws JobPersistenceException, TransientException {
        log.debug("getDetails: " + job.getID());
        expectPersistentJob(job);
        try {
            JobDetailSelectStatementCreator sc = new JobDetailSelectStatementCreator();
            sc.setJobID(job.getID());
            jdbc.query(sc, new DetailExtractor(jobSchema, job)); // ignore unnecessary return value
            prof.checkpoint("JobDetailSelectStatementCreator");
        } catch (Throwable t) {
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to get job details: " + job.getID(), t);
            } else {
                throw new JobPersistenceException("failed to get job details: " + job.getID(), t);
            }
        }
    }

    /**
     * Get the current execution phase.
     *
     * @param jobID
     * @return the current phase
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public ExecutionPhase getPhase(String jobID)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("getPhase: " + jobID);
        try {
            JobPhaseSelectStatementCreator sc = new JobPhaseSelectStatementCreator();
            sc.setJobID(jobID);
            ExecutionPhase ret = (ExecutionPhase) jdbc.query(sc, new PhaseExtractor());
            prof.checkpoint("JobPhaseSelectStatementCreator");
            if (ret != null) {
                return ret;
            }
        } catch (Throwable t) {
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to get job phase: " + jobID, t);
            } else {
                throw new JobPersistenceException("failed to get job phase: " + jobID, t);
            }
        }
        throw new JobNotFoundException(jobID);
    }

    /**
     * Set the phase of the specified job.
     *
     * @param jobID
     * @param ep
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public void set(String jobID, ExecutionPhase ep)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        set(jobID, null, ep, null, null, null);
    }

    /**
     * Conditionally set the phase from <em>start</em> to <em>end</em>. The transition is
     * successful IFF the current phase is equal to the starting phase.
     *
     * @param jobID
     * @param start
     * @param end
     * @param date
     * @return the resulting phase or null if the the transition was not successful.
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public ExecutionPhase set(String jobID, ExecutionPhase start, ExecutionPhase end, Date date)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("set: " + jobID + "," + start + "," + end + "," + date);
        return set(jobID, start, end, null, null, date);
    }

    /**
     * Conditionally set the phase from <em>start</em> to <em>end</em> and set the
     * results. The transition is successful IFF the current phase is equal to the
     * starting phase. Results are only set if the phase transition is successful.
     * Note: this method is unconditional when <em>start</em> is null.
     *
     * @param jobID
     * @param start
     * @param end
     * @param date
     * @param results
     * @return the resulting phase or null if the the transition was not successful.
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public ExecutionPhase set(String jobID, ExecutionPhase start, ExecutionPhase end, List<Result> results, Date date)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("set: " + jobID + "," + start + "," + end + ", <results>," + date);
        return set(jobID, start, end, null, results, date);
    }

    /**
     * Conditionally set the phase from <em>start</em> to <em>end</em> and set the
     * error summary. The transition is successful IFF the current phase is equal to the
     * starting phase. Results are only set if the phase transition is successful.
     * Note: this method is unconditional when <em>start</em> is null.
     *
     * @param jobID
     * @param start
     * @param end
     * @param error
     * @param date
     * @return the resulting phase or null if the the transition was not successful.
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public ExecutionPhase set(String jobID, ExecutionPhase start, ExecutionPhase end, ErrorSummary error, Date date)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("set: " + jobID + "," + start + "," + end + ", <error>," + date);
        return set(jobID, start, end, error, null, date);
    }

    /**
     * Conditionally set the phase from <em>start</em> to <em>end</em> and set the
     * results and/or error. The transition is successful IFF the current phase is equal to the
     * starting phase. Results are only set if the phase transition is successful.
     * Note: this method is unconditional when <em>start</em> is null.
     *
     * @param jobID
     * @param start
     * @param end
     * @param error
     * @param results
     * @param date
     * @return the resulting phase or null if the the transition was not successful.
     * @throws JobNotFoundException
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public ExecutionPhase set(String jobID, ExecutionPhase start, ExecutionPhase end, ErrorSummary error, List<Result> results, Date date)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("set: " + jobID + "," + start + "," + end + ", <error>, <results>, " + date);
        boolean txn = false; // need explicit transaction or autocommit?
        if (results != null && results.size() > 0) {
            txn = true;
        }
        try {
            if (txn) {
                startTransaction();
            }
            JobPhaseUpdateStatementCreator sc = new JobPhaseUpdateStatementCreator();
            sc.setValues(jobID, start, end, error, date);
            int n = jdbc.update(sc);
            prof.checkpoint("JobPhaseUpdateStatementCreator");
            if (n == 1) {
                if (results != null && results.size() > 0) {
                    DetailInsertStatementCreator disc = new DetailInsertStatementCreator();
                    for (Result r : results) {
                        disc.setValues(jobID, r);
                        jdbc.update(disc);
                        prof.checkpoint("DetailInsertStatementCreator");
                    }
                }
            }
            if (txn) {
                commitTransaction();
                prof.checkpoint("commit.JobPhaseUpdateStatementCreator");
            }
            if (n == 1) {
                return end;
            }
            return null;
        } catch (Throwable t) {
            log.error("rollback for job: " + jobID, t);
            try {
                if (txn) {
                    rollbackTransaction();
                    prof.checkpoint("rollback.JobPhaseUpdateStatementCreator");
                }
            } catch (Throwable oops) {
                log.error("failed to rollback transaction", oops);
            }

            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to persist job: " + jobID, t);
            } else {
                throw new JobPersistenceException("failed to persist job: " + jobID, t);
            }
        } finally {
            if (transactionStatus != null) {
                try {
                    log.warn("put: BUG - transaction still open in finally... calling rollback");
                    if (txn) {
                        rollbackTransaction();
                    }
                } catch (Throwable oops) {
                    log.error("failed to rollback transaction in finally", oops);
                }
            }
        }
    }

    /**
     * Iterate over the jobs owned by the user in the subject contained in the
     * access control context.
     *
     * @param phases Show only these phases
     * @param after Only show jobs after this time
     * @param last Show the last <i>last</i> jobs, ordererd by startTime ascending
     * @return job iterator
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public Iterator<JobRef> iterator(String requestPath, List<ExecutionPhase> phases, Date after, Integer last)
            throws TransientException, JobPersistenceException {
        AccessControlContext acContext = AccessController.getContext();
        Subject subject = Subject.getSubject(acContext);
        return iterator(subject, requestPath, phases, after, last);
    }

    /**
     * Iterator over jobs owned by the specified owner.
     *
     * @param subject
     * @param phases Show only these phases
     * @param after Only show jobs after this time
     * @param last Show the last <i>last</i> jobs, ordererd by startTime ascending
     * @return job iterator
     */
    public Iterator<JobRef> iterator(Subject subject, String requestPath, List<ExecutionPhase> phases, Date after, Integer last)
            throws TransientException, JobPersistenceException {
        Object ownerID = identManager.toOwner(subject);
        log.debug("iterator(" + ownerID + ")");

        try {
            JobListIterator jobListIterator = new JobListIterator(jdbc, ownerID, requestPath, phases, after, last);
            prof.checkpoint("JobListStatementCreator");
            return jobListIterator;
        } catch (Throwable t) {
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to get job list for owner: " + ownerID, t);
            } else {
                throw new JobPersistenceException("failed to get job list for owner: " + ownerID, t);
            }
        }
    }

    /**
     * Persist the specified job.
     *
     * @param job
     * @return the job (possibly modified)
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public Job put(Job job)
            throws JobPersistenceException, TransientException {
        try {
            boolean update = (job.getID() != null);
            if (!update) {
                JobPersistenceUtil.assignID(job, idGenerator.getID());
                job.setCreationTime(new Date());
            }
            log.debug("put: " + job.getID());

            startTransaction();
            prof.checkpoint("start.JobPutStatementCreator");

            JobPutStatementCreator npsc = new JobPutStatementCreator(update);
            npsc.setValues(job);
            while (true) {
                try {
                    jdbc.update(npsc);
                    break;
                } catch (org.springframework.dao.DuplicateKeyException e) {
                    log.warn("Re-try on job ID collision: " + job.getID());
                    try {
                        rollbackTransaction();
                        prof.checkpoint("rollback.JobPutStatementCreator");
                    } catch (Throwable oops) {
                        log.error("failed to rollback transaction", oops);
                        throw e;
                    }
                    log.debug("Start new transaction");
                    startTransaction();
                    JobPersistenceUtil.assignID(job, idGenerator.getID());
                    npsc.setValues(job);
                }
            }

            prof.checkpoint("JobPutStatementCreator");

            int numP = 0;
            if (job.getParameterList() != null) {
                numP = job.getParameterList().size();
            }
            int numR = 0;
            if (job.getResultsList() != null) {
                numR = job.getResultsList().size();
            }

            if (numP + numR > 0) {
                DetailDeleteStatementCreator dd = new DetailDeleteStatementCreator();
                dd.setJobID(job.getID());
                jdbc.update(dd);
                prof.checkpoint("DetailDeleteStatementCreator");
            }

            DetailInsertStatementCreator disc = new DetailInsertStatementCreator();
            if (numP > 0) {
                Iterator<Parameter> pi = job.getParameterList().iterator();
                while (pi.hasNext()) {
                    Parameter param = pi.next();
                    disc.setValues(job.getID(), param);
                    jdbc.update(disc);
                    prof.checkpoint("DetailInsertStatementCreator");
                }
            }
            if (numR > 0) {
                Iterator<Result> ri = job.getResultsList().iterator();
                while (ri.hasNext()) {
                    Result res = ri.next();
                    disc.setValues(job.getID(), res);
                    jdbc.update(disc);
                    prof.checkpoint("DetailInsertStatementCreator");
                }
            }

            commitTransaction();
            prof.checkpoint("commit.JobPutStatementCreator");

            // OK to modify the job now
            Job ret = job; // side effect
            if (ret.ownerID != null) {
                ret.owner = identManager.toSubject(ret.ownerID);
                ret.ownerID = identManager.toOwner(ret.owner); // type change
                ret.ownerDisplay = identManager.toDisplayString(ret.owner);
                prof.checkpoint("IdentityManager.toSubject");
            }
            
            return job;
        } catch (Throwable t) {
            log.error("rollback for job: " + job.getID(), t);
            try {
                rollbackTransaction();
                prof.checkpoint("rollback.JobPutStatementCreator");
            } catch (Throwable oops) {
                log.error("failed to rollback transaction", oops);
            }
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to persist job: " + job.getID(), t);
            } else {
                throw new JobPersistenceException("failed to persist job: " + job.getID(), t);
            }
        } finally {
            if (transactionStatus != null) {
                try {
                    log.warn("put: BUG - transaction still open in finally... calling rollback");
                    rollbackTransaction();
                } catch (Throwable oops) {
                    log.error("failed to rollback transaction in finally", oops);
                }
            }
        }
    }

    public void addParameters(String jobID, List<Parameter> params)
            throws JobNotFoundException, JobPersistenceException, TransientException {
        log.debug("addParameters: " + jobID);
        try {
            if (params != null && params.size() > 0) {
                startTransaction();
                DetailInsertStatementCreator disc = new DetailInsertStatementCreator();
                for (Parameter p : params) {
                    disc.setValues(jobID, p);
                    jdbc.update(disc);
                    prof.checkpoint("DetailInsertStatementCreator");
                }
                commitTransaction();
                prof.checkpoint("commit.DetailInsertStatementCreator");
            }
        } catch (DataIntegrityViolationException notFound) {
            throw new JobNotFoundException("not found: " + jobID);
        } catch (Throwable t) {
            log.error("rollback for job: " + jobID, t);
            try {
                rollbackTransaction();
                prof.checkpoint("rollback.DetailInsertStatementCreator");
            } catch (Throwable oops) {
                log.error("failed to rollback transaction", oops);
            }
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to persist job parameters: " + jobID, t);
            } else {
                throw new JobPersistenceException("failed to persist job parameters: " + jobID, t);
            }
        } finally {
            if (transactionStatus != null) {
                try {
                    log.warn("put: BUG - transaction still open in finally... calling rollback");
                    rollbackTransaction();
                } catch (Throwable oops) {
                    log.error("failed to rollback transaction in finally", oops);
                }
            }
        }
    }

    /**
     * Delete the specified job.
     *
     * @param jobID
     * @throws JobPersistenceException
     * @throws TransientException
     */
    public void delete(String jobID)
            throws JobPersistenceException, TransientException {
        log.debug("delete: " + jobID);
        try {
            //startTransaction();
            JobDeleteStatementCreator jdl = new JobDeleteStatementCreator();
            jdl.setJobID(jobID);
            jdbc.update(jdl);
            prof.checkpoint("JobDeleteStatementCreator");
            //commitTransaction();
        } catch (Throwable t) {
            log.error("failed to delete job", t);
            if (transactionStatus != null) {
                try {
                    rollbackTransaction();
                    prof.checkpoint("rollback.DetailInsertStatementCreator");
                } catch (Throwable oops) {
                    log.error("failed to rollback transaction", oops);
                }
            }
            if (DBUtil.isTransientDBException(t)) {
                throw new TransientException("failed to delete job: " + jobID, t);
            } else {
                throw new JobPersistenceException("failed to delete job: " + jobID, t);
            }
        } finally {
            if (transactionStatus != null) {
                try {
                    log.warn("delete - BUG - transaction still open in finally... calling rollback");
                    rollbackTransaction();
                } catch (Throwable oops) {
                    log.error("failed to rollback transaction in finally", oops);
                }
            }
        }
    }

    protected void expectPersistentJob(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("job cannot be null");
        }
        if (job.getLastModified() == null) {
            throw new IllegalArgumentException("node is not a persistent job: has null lastModified");
        }
    }

    private void appendColumnNames(StringBuilder sb, List<String> arr, int start) {
        for (int c = start; c < arr.size(); c++) {
            String col = arr.get(c);
            if (c > start) {
                sb.append(",");
            }
            sb.append(col);
        }
    }

    private void appendStatementParams(StringBuilder sb, int num) {
        for (int c = 0; c < num; c++) {
            if (c > 0) {
                sb.append(",");
            }
            sb.append("?");
        }
    }

    private int setString(PreparedStatement ps, int col, String tableName, String columnName, String value, StringBuilder sb)
            throws SQLException {
        Integer limit = jobSchema.getColumnLength(tableName, columnName);

        if (limit == null) {
            // single column
            if (value == null) {
                ps.setNull(col, Types.VARCHAR);
            } else {
                ps.setString(col, value);
            }
            sb.append(value);
            sb.append(",");
            return 1;
        }

        // pair of columns
        if (value == null) {
            ps.setNull(col, Types.VARCHAR);
            ps.setNull(col + 1, Types.VARCHAR);
            sb.append("null,null,");
        } else if (value.length() <= limit) {
            ps.setString(col, value);
            ps.setNull(col + 1, Types.VARCHAR);
            sb.append(value);
            sb.append(",null,");
        } else {
            ps.setNull(col, Types.VARCHAR);
            ps.setString(col + 1, value);
            sb.append("null,");
            sb.append(value);
            sb.append(",");
        }
        return 2;
    }

    class JobDeleteStatementCreator implements PreparedStatementCreator {

        private String jobID;

        public JobDeleteStatementCreator() {
        }

        public void setJobID(String jobID) {
            this.jobID = jobID;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = "UPDATE " + jobSchema.jobTable + " SET deletedByUser = 1 WHERE jobID = ?";
            log.debug(sql);
            log.debug(jobID);
            PreparedStatement prep = conn.prepareStatement(sql);
            prep.setString(1, jobID);
            return prep;
        }
    }

    class JobPutStatementCreator implements PreparedStatementCreator {

        private boolean update;
        private Job job;
        private String sql;

        public JobPutStatementCreator(boolean update) {
            this.update = update;
            if (update) {
                this.sql = getUpdateSQL();
            } else {
                this.sql = getInsertSQL();
            }
        }

        public void setValues(Job job) {
            this.job = job;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            log.debug(sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            StringBuilder sb = new StringBuilder();

            int col = 1;
            if (!update) {
                log.debug("jobID: " + col);
                ps.setString(col++, job.getID());
                sb.append(job.getID());
                sb.append(",");

                Timestamp ctTs = new Timestamp(job.getCreationTime().getTime());
                ps.setTimestamp(col++, ctTs, cal);
                sb.append(dateFormat.format(job.getCreationTime()));
                sb.append(",");
            }

            ps.setString(col++, job.getExecutionPhase().name());
            sb.append(job.getExecutionPhase().name());
            sb.append(",");

            if (job.getExecutionDuration() != null) {
                ps.setLong(col++, job.getExecutionDuration());
                sb.append(job.getExecutionDuration());
                sb.append(",");
            } else {
                ps.setNull(col++, Types.BIGINT);
                sb.append("null,");
            }

            if (job.getDestructionTime() != null) {
                Timestamp ts = new Timestamp(job.getDestructionTime().getTime());
                ps.setTimestamp(col++, ts, cal);
                sb.append(dateFormat.format(job.getDestructionTime()));
                sb.append(",");
            } else {
                ps.setNull(col++, Types.TIMESTAMP);
                sb.append("null,");
            }

            if (job.getQuote() != null) {
                Timestamp ts = new Timestamp(job.getQuote().getTime());
                ps.setTimestamp(col++, ts, cal);
                sb.append(dateFormat.format(job.getQuote()));
                sb.append(",");
            } else {
                ps.setNull(col++, Types.TIMESTAMP);
                sb.append("null,");
            }

            if (job.getStartTime() != null) {
                Timestamp ts = new Timestamp(job.getStartTime().getTime());
                ps.setTimestamp(col++, ts, cal);
                sb.append(dateFormat.format(job.getStartTime()));
                sb.append(",");
            } else {
                ps.setNull(col++, Types.TIMESTAMP);
                sb.append("null,");
            }

            if (job.getEndTime() != null) {
                Timestamp ts = new Timestamp(job.getEndTime().getTime());
                ps.setTimestamp(col++, ts, cal);
                sb.append(dateFormat.format(job.getEndTime()));
                sb.append(",");
            } else {
                ps.setNull(col++, Types.TIMESTAMP);
                sb.append("null,");
            }
            log.debug("error summary: " + col);
            String errorMsg = null;
            String errorType = null;
            String errorURL = null;
            if (job.getErrorSummary() != null) {
                errorMsg = job.getErrorSummary().getSummaryMessage();
                errorType = job.getErrorSummary().getErrorType().name();
                if (job.getErrorSummary().getDocumentURL() != null) {
                    errorURL = job.getErrorSummary().getDocumentURL().toExternalForm();
                }
            }
            col += setString(ps, col, jobSchema.jobTable, "error_summaryMessage", errorMsg, sb);
            col += setString(ps, col, jobSchema.jobTable, "error_type", errorType, sb);
            col += setString(ps, col, jobSchema.jobTable, "error_documentURL", errorURL, sb);

            log.debug("owner: " + col);
            if (job.ownerID != null) {
                Object ownerObject = job.ownerID;
                int otype = Types.VARCHAR;
                Object oval = ownerObject;
                if (jobSchema.storeOwnerASCII) { // standard usage now
                    otype = Types.VARCHAR;
                    oval = ownerObject.toString();
                } else if (oval instanceof String) {
                    otype = Types.VARCHAR;
                } else if (oval instanceof Integer) {
                    otype = Types.INTEGER; // deprecated
                } else if (oval instanceof Long) {
                    otype = Types.BIGINT; // deprecated
                } else {
                    //throw new RuntimeException("BUG: cannot map " + oval.getClass().getName() + " to an SQL TYPE");
                    otype = Types.OTHER; // hope the JDBC driver can handle it
                }

                ps.setObject(col++, oval, otype);
                sb.append(oval);
                sb.append(",");
            } else {
                ps.setNull(col++, Types.OTHER); // hope the JDBC driver can handle it
                sb.append("null,");
            }

            log.debug("runID: " + col);
            ps.setString(col++, job.getRunID());
            sb.append(job.getRunID());
            sb.append(",");

            col += setString(ps, col, jobSchema.jobTable, "requestPath", job.getRequestPath(), sb);

            col += setString(ps, col, jobSchema.jobTable, "remoteIP", job.getRemoteIP(), sb);

            log.debug("jobInfo: " + col);
            String jiContent = null;
            String jiContentType = null;

            if (job.getJobInfo() != null) {
                jiContent = job.getJobInfo().getContent();
                jiContentType = job.getJobInfo().getContentType();

            }
            col += setString(ps, col, jobSchema.jobTable, "jobInfo_content", jiContent, sb);
            col += setString(ps, col, jobSchema.jobTable, "jobInfo_contentType", jiContentType, sb);
            if (job.getJobInfo() != null && job.getJobInfo().getValid() != null) {
                int jiValid = 0;
                if (job.getJobInfo().getValid()) {
                    jiValid = 1;
                }
                ps.setInt(col++, jiValid);
                sb.append(Integer.toString(jiValid));
                sb.append(",");
            } else {
                ps.setNull(col++, Types.TINYINT);
                sb.append("null,");
            }

            log.debug("lastModified: " + col);
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            ps.setTimestamp(col++, ts, cal);
            sb.append(dateFormat.format(now));
            sb.append(",");

            if (update) {
                log.debug("update - jobID: " + col);
                ps.setString(col++, job.getID());
                sb.append(",");
                sb.append(job.getID());
            }

            log.debug(sb);
        }

        private String getInsertSQL() {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(jobSchema.jobTable);
            sb.append(" (");
            appendColumnNames(sb, jobSchema.jobColumns, 0);
            sb.append(") VALUES (");
            appendStatementParams(sb, jobSchema.jobColumns.size());
            sb.append(")");
            return sb.toString();
        }

        private String getUpdateSQL() {
            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE ");
            sb.append(jobSchema.jobTable);

            // column-list syntax as in insert is not uniformly supported
            //sb.append(" SET (");
            //appendColumnNames(sb, jobSchema.jobColumns, 1);
            //sb.append(") = (");
            //appendStatementParams(sb, jobSchema.jobColumns.size());
            //sb.append(")");
            // don't include jobID (index 0) or creation time (index size-1)
            sb.append(" SET ");
            for (int i = 2; i < jobSchema.jobColumns.size(); i++) {
                if (i > 2) {
                    sb.append(",");
                }
                sb.append(jobSchema.jobColumns.get(i));
                sb.append(" = ?");
            }
            sb.append(" WHERE jobID = ?");
            return sb.toString();
        }
    }

    class JobListStatementCreator implements PreparedStatementCreator {

        private Object ownerID;
        private String requestPath;
        private List<ExecutionPhase> phases;
        private Date after;
        private Integer last;
        private String lastJobID;
        private Date lastCreationTime;

        private Calendar utc = Calendar.getInstance(DateUtil.UTC);

        public JobListStatementCreator(String lastJobID, Date lastCreationTime, 
                Object ownerID, String requestPath, List<ExecutionPhase> phases, Date after, Integer last) {
            this.lastJobID = lastJobID;
            this.lastCreationTime = lastCreationTime;
            this.requestPath = requestPath;
            this.ownerID = ownerID;
            this.phases = phases;
            this.after = after;
            this.last = last;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            String sql = getSQL();
            log.debug(sql);
            PreparedStatement ret = conn.prepareStatement(sql);
            int arg = 1;
            if (ownerID != null) {
                int otype = Types.VARCHAR;
                Object oval = ownerID;
                if (jobSchema.storeOwnerASCII) {
                    otype = Types.VARCHAR;
                    oval = ownerID.toString();
                } else if (oval instanceof String) {
                    otype = Types.VARCHAR;
                } else if (oval instanceof Integer) {
                    otype = Types.INTEGER;
                } else if (oval instanceof Long) {
                    otype = Types.BIGINT;
                } else {
                    //throw new RuntimeException("BUG: cannot map " + oval.getClass().getName() + " to an SQL TYPE");
                    otype = Types.OTHER; // hope the JDBC driver can handle it
                }
                log.debug(arg + " : " + oval);
                ret.setObject(arg++, oval, otype);
            }

            if (phases != null && !phases.isEmpty()) {
                for (ExecutionPhase ep : phases) {
                    log.debug(arg + " : " + ep);
                    ret.setString(arg++, ep.getValue());
                }
            } else {
                log.debug(arg + " : " + ExecutionPhase.ARCHIVED);
                ret.setString(arg++, ExecutionPhase.ARCHIVED.getValue());
            }
            if (requestPath != null) {
                log.debug(arg + " : " + requestPath);
                ret.setString(arg++, requestPath);
            }
            if (after != null) {
                ret.setTimestamp(arg++, new Timestamp(after.getTime()), utc);
            }
            if (last != null) {
                if (lastCreationTime != null) {
                    ret.setTimestamp(arg++, new Timestamp(lastCreationTime.getTime()), utc);
                }
            } else if (lastJobID != null) {
                ret.setString(arg++, lastJobID);
            }

            return ret;
        }

        protected String getSQL() {
            int rowLimit = BATCH_SIZE;
            if (last != null && last < BATCH_SIZE) {
                rowLimit = last;
            }
            StringBuilder sb = new StringBuilder();

            sb.append("SELECT ");
            if (jobSchema.limitWithTop) {
                sb.append("TOP ").append(rowLimit);
            }
            sb.append(" jobID, executionPhase, creationTime, runID, ownerID FROM ");
            sb.append(jobSchema.jobTable);
            sb.append(" WHERE deletedByUser = 0");
            if (ownerID != null) {
                sb.append(" AND ownerID = ?");
            }

            if (phases != null && !phases.isEmpty()) {
                sb.append(" AND executionPhase IN (");
                Iterator<ExecutionPhase> i = phases.iterator();
                while (i.hasNext()) {
                    i.next();
                    sb.append("?");
                    if (i.hasNext()) {
                        sb.append(",");
                    }
                }
                sb.append(")");
            } else {
                sb.append(" AND executionPhase != ?");
            }

            if (requestPath != null) {
                sb.append(" AND requestPath = ?");
            }

            if (after != null) {
                sb.append(" AND creationTime > ?");
            }
            if (last != null) {
                log.debug("batch and sort by creationTime");
                if (lastCreationTime != null) {
                    sb.append(" AND creationTime <= ?");
                } else {
                    sb.append(" AND creationTime IS NOT NULL");
                }
                sb.append(" ORDER BY creationTime DESC, jobID ASC"); // fully consistent order for iterator duplicate filter
            } else {
                log.debug("batch and sort by jobID");
                if (lastJobID != null) {
                    sb.append(" AND jobID > ?");
                }
                sb.append(" ORDER by jobID ASC");
            }

            if (!jobSchema.limitWithTop) {
                sb.append(" LIMIT ").append(rowLimit);
            }
            return sb.toString();
        }
    }

    class JobSelectStatementCreator implements PreparedStatementCreator {

        private String jobID;
        private String sql;

        public JobSelectStatementCreator() {
            this.sql = getSQL();
        }

        public void setJobID(String jobID) {
            this.jobID = jobID;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            PreparedStatement ret = conn.prepareStatement(sql);
            ret.setString(1, jobID);
            log.debug(sql);
            log.debug(jobID);
            return ret;
        }

        String getSQL() {
            if (sql != null) {
                return sql;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            appendColumnNames(sb, jobSchema.jobColumns, 0);
            sb.append(" FROM ");
            sb.append(jobSchema.jobTable);
            sb.append(" WHERE jobID = ? AND deletedByUser = 0");
            return sb.toString();
        }
    }

    // select parameters and results from the param table
    class JobDetailSelectStatementCreator implements PreparedStatementCreator {

        private String jobID;
        private String sql;

        public JobDetailSelectStatementCreator() {
            this.sql = getSQL();
        }

        public void setJobID(String jobID) {
            this.jobID = jobID;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            PreparedStatement ret = conn.prepareStatement(sql);
            ret.setString(1, jobID);
            log.debug(sql);
            log.debug(jobID);
            return ret;
        }

        String getSQL() {
            if (sql != null) {
                return sql;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            appendColumnNames(sb, jobSchema.detailColumns, 0);
            sb.append(" FROM ");
            sb.append(jobSchema.detailTable);
            sb.append(" WHERE jobID = ?");
            return sb.toString();
        }
    }

    class DetailInsertStatementCreator implements PreparedStatementCreator {

        private String jobID;
        private Parameter param;
        private Result result;

        private String sql;

        public DetailInsertStatementCreator() {
            this.sql = getSQL();
        }

        public void setValues(String jobID, Parameter p) {
            this.jobID = jobID;
            this.param = p;
            this.result = null;
        }

        public void setValues(String jobID, Result r) {
            this.jobID = jobID;
            this.param = null;
            this.result = r;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
                throws SQLException {
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);
            StringBuilder sb = new StringBuilder();
            ps.setString(1, jobID);
            sb.append(jobID);
            sb.append(",");
            if (param != null) {
                ps.setString(2, TYPE_PARAMETER);
                ps.setString(3, param.getName());
                sb.append(TYPE_PARAMETER);
                sb.append(",");
                sb.append(param.getName());
                sb.append(",");
                setString(ps, 4, jobSchema.detailTable, "value", param.getValue(), sb);
            } else if (result != null) {
                ps.setString(2, TYPE_RESULT);
                ps.setString(3, result.getName());
                sb.append(TYPE_RESULT);
                sb.append(",");
                sb.append(result.getName());
                sb.append(",");
                setString(ps, 4, jobSchema.detailTable, "value", result.getURI().toASCIIString(), sb);
            }
            log.debug(sb);
            return ps;
        }

        private String getSQL() {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(jobSchema.detailTable);
            sb.append(" (");
            appendColumnNames(sb, jobSchema.detailColumns, 0);
            sb.append(") VALUES (");
            appendStatementParams(sb, jobSchema.detailColumns.size());
            sb.append(")");

            return sb.toString();
        }
    }

    class DetailDeleteStatementCreator implements PreparedStatementCreator {

        private String jobID;
        private String sql;

        public DetailDeleteStatementCreator() {
            this.sql = getSQL();
        }

        public void setJobID(String jobID) {
            this.jobID = jobID;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
                throws SQLException {
            log.debug(sql);
            log.debug("values: " + jobID);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, jobID);
            return ps;
        }

        private String getSQL() {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM ");
            sb.append(jobSchema.detailTable);
            sb.append(" WHERE jobID = ?");
            return sb.toString();
        }
    }

    class JobPhaseSelectStatementCreator implements PreparedStatementCreator {

        private String jobID;
        private String sql;

        public JobPhaseSelectStatementCreator() {
            this.sql = "SELECT executionPhase FROM " + jobSchema.jobTable
                    + " WHERE jobID = ?";
        }

        public void setJobID(String jobID) {
            this.jobID = jobID;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            log.debug(sql);
            log.debug("values: " + jobID);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, jobID);
            return ps;
        }

    }

    class JobPhaseUpdateStatementCreator implements PreparedStatementCreator {

        private String jobID;
        private ExecutionPhase start;
        private ExecutionPhase end;
        private ErrorSummary error;
        private Date date;

        public JobPhaseUpdateStatementCreator() {
        }

        public void setValues(String jobID, ExecutionPhase start, ExecutionPhase end,
                ErrorSummary error, Date date) {
            this.jobID = jobID;
            this.start = start;
            this.end = end;
            this.error = error;
            this.date = date;
        }

        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            String sql = getUpdateSQL();
            log.debug(sql);
            PreparedStatement ps = conn.prepareStatement(sql);

            int col = 1;

            ps.setString(col++, end.name());
            sb.append(end.name());
            sb.append(",");

            log.debug("error summary: " + col);
            String errorMsg = null;
            String errorType = null;
            String errorURL = null;
            if (error != null) {
                errorMsg = error.getSummaryMessage();
                errorType = error.getErrorType().name();
                if (error.getDocumentURL() != null) {
                    errorURL = error.getDocumentURL().toExternalForm();
                }
                col += setString(ps, col, jobSchema.jobTable, "error_summaryMessage", errorMsg, sb);
                col += setString(ps, col, jobSchema.jobTable, "error_type", errorType, sb);
                col += setString(ps, col, jobSchema.jobTable, "error_documentURL", errorURL, sb);
            }
            if (date != null) {
                Timestamp ts = new Timestamp(date.getTime());
                ps.setTimestamp(col++, ts, cal);
                sb.append(dateFormat.format(date));
                sb.append(",");
            }

            Date now = new Date();
            Timestamp nowts = new Timestamp(now.getTime());
            ps.setTimestamp(col++, nowts, cal);
            sb.append(dateFormat.format(now));
            sb.append(",");

            ps.setString(col++, jobID);
            sb.append(jobID);
            sb.append(",");
            if (start != null) {
                ps.setString(col++, start.name());
                sb.append(start.name());
            }

            log.debug(sb.toString());
            return ps;
        }

        private String getUpdateSQL() {
            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE ");
            sb.append(jobSchema.jobTable);
            sb.append(" SET executionPhase = ?");
            if (error != null) {
                sb.append(", error_summaryMessage = ?");
                String alt = jobSchema.getAlternateColumn(jobSchema.jobTable, "error_summaryMessage");
                if (alt != null) {
                    sb.append(",");
                    sb.append(alt);
                    sb.append("= ?");
                }
                sb.append(", error_type = ?");
                alt = jobSchema.getAlternateColumn(jobSchema.jobTable, "error_type");
                if (alt != null) {
                    sb.append(",");
                    sb.append(alt);
                    sb.append("= ?");
                }
                sb.append(", error_documentURL = ?");
                alt = jobSchema.getAlternateColumn(jobSchema.jobTable, "error_documentURL");
                if (alt != null) {
                    sb.append(",");
                    sb.append(alt);
                    sb.append("= ?");
                }
            }
            if (date != null) {
                if (ExecutionPhase.EXECUTING.equals(end)) {
                    sb.append(", startTime = ?");
                } else if (JobPersistenceUtil.isFinalPhase(end)) {
                    sb.append(", endTime = ?");
                } else {
                    date = null; // ignore
                }
            }
            sb.append(", lastModified = ?");
            sb.append(" WHERE jobID = ?");
            if (start != null) {
                sb.append(" AND executionPhase = ?");
            }
            return sb.toString();
        }
    }

    /**
     * Get the string value from the specified column. The default implementation
     * simply calls rs.getString(columnName) which should work in most cases.
     * Applications could override this method to check additional alternate
     * columns, for example if the value of some strings might be stored in a
     * TEXT type column instead of a VARCHAR column.
     *
     * @param rs
     * @param tableName
     * @param columnName
     * @return
     * @throws SQLException
     */
    protected String getString(ResultSet rs, String tableName, String columnName)
            throws SQLException {
        String value = rs.getString(columnName);
        if (value == null) {
            String extCol = jobSchema.getAlternateColumn(tableName, columnName);
            if (extCol != null) {
                value = rs.getString(extCol);
            }
        }
        return value;
    }

    // extract a single job job from the result set
    private class JobExtractor implements ResultSetExtractor {

        private JobSchema js;

        public JobExtractor(JobSchema js) {
            this.js = js;
        }

        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (rs.next()) {
                final String jobID = rs.getString("jobID");
                final ExecutionPhase executionPhase = ExecutionPhase.valueOf(rs.getString("executionPhase").toUpperCase());
                final long executionDuration = rs.getLong("executionDuration");
                final Date destructionTime = rs.getTimestamp("destructionTime", Calendar.getInstance(DateUtil.UTC));
                final Date quote = rs.getTimestamp("quote", Calendar.getInstance(DateUtil.UTC));
                final Date startTime = rs.getTimestamp("startTime", Calendar.getInstance(DateUtil.UTC));
                final Date endTime = rs.getTimestamp("endTime", Calendar.getInstance(DateUtil.UTC));
                final Date creationTime = rs.getTimestamp("creationTime", Calendar.getInstance(DateUtil.UTC));
                
                ErrorSummary errorSummary = null;
                String errorMsg = getString(rs, jobSchema.jobTable, "error_summaryMessage");
                String errorTypeStr = getString(rs, jobSchema.jobTable, "error_type");
                ErrorType errorType = null;
                if (errorTypeStr != null) {
                    errorType = ErrorType.valueOf(errorTypeStr);
                }
                if (errorMsg != null) {
                    URL errorUrl = null;
                    try {
                        String surl = getString(rs, jobSchema.jobTable, "error_documentURL");
                        errorUrl = new URL(surl);
                    } catch (MalformedURLException e) {
                        errorUrl = null;
                    }
                    errorSummary = new ErrorSummary(errorMsg, errorType, errorUrl);
                }

                final Object ownerID = rs.getObject("ownerID");
                String runID = getString(rs, jobSchema.jobTable, "runID");
                String requestPath = getString(rs, jobSchema.jobTable, "requestPath");
                String remoteIP = getString(rs, jobSchema.jobTable, "remoteIP");
                String content = getString(rs, jobSchema.jobTable, "jobInfo_content");
                String contentType = getString(rs, jobSchema.jobTable, "jobInfo_contentType");

                // JobInfo valid
                Boolean valid = null;
                int i = rs.getInt("jobInfo_valid");
                if (!rs.wasNull()) {
                    valid = i == 0 ? false : true;
                }

                // JobInfo
                JobInfo jobInfo;
                if (content == null && contentType == null) {
                    jobInfo = null;
                } else {
                    jobInfo = new JobInfo(content, contentType, valid);
                }

                Date lastModified = rs.getTimestamp("lastModified", cal);

                Job job = new Job(executionPhase, executionDuration, destructionTime,
                        quote, startTime, endTime, creationTime, errorSummary, runID,
                        requestPath, remoteIP, jobInfo, null, null);
                JobPersistenceUtil.assignID(job, jobID);
                assignLastModified(job, lastModified);
                job.ownerID = ownerID; // string at this point (see get method above)
                return job;
            }
            return null;
        }

        private void assignLastModified(Job job, Date lastModified) {
            try {
                Field f = job.getClass().getDeclaredField("lastModified");
                f.setAccessible(true);
                f.set(job, lastModified);
            } catch (NoSuchFieldException fex) {
                throw new RuntimeException("BUG", fex);
            } catch (IllegalAccessException bug) {
                throw new RuntimeException("BUG", bug);
            }
        }
    }

    private class JobListIterator implements Iterator<JobRef> {

        private JdbcTemplate jdbcTemplate;
        private Iterator<JobRef> jobRefIterator;
        private String lastJobID = null;
        private Date lastCreationTime = null;
        private Object ownerID;
        private String requestPath;
        private List<ExecutionPhase> phases;
        private Date after;
        private Integer last;
        private long count = 0;

        JobListIterator(JdbcTemplate jdbc, Object ownerID, String requestPath, List<ExecutionPhase> phases, Date after, Integer last) {
            this.jdbcTemplate = jdbc;
            this.ownerID = ownerID;
            this.requestPath = requestPath;
            this.phases = phases;
            this.after = after;
            this.last = last;
            this.jobRefIterator = getNextBatchIterator();
        }

        @Override
        public boolean hasNext() {
            if (last != null && count >= last) {
                return false;
            }

            if (!jobRefIterator.hasNext()) {
                this.jobRefIterator = getNextBatchIterator();
            }

            return this.jobRefIterator.hasNext();
        }

        @Override
        public JobRef next() {
            JobRef ret = this.jobRefIterator.next();
            count++;
            this.lastJobID = ret.getJobID();
            this.lastCreationTime = ret.getCreationTime();
            return ret;
        }

        @SuppressWarnings("unchecked")
        private Iterator<JobRef> getNextBatchIterator() {
            JobListStatementCreator sc = new JobListStatementCreator(lastJobID, lastCreationTime, ownerID, requestPath, phases, after, last);
            final Calendar utc = Calendar.getInstance(DateUtil.UTC);
            List<JobRef> jobs = this.jdbcTemplate.query(sc, new RowMapper<JobRef>() {
                public JobRef mapRow(ResultSet rs, int rowNum) throws SQLException {
                    ExecutionPhase executionPhase = ExecutionPhase.valueOf(rs.getString("executionPhase").toUpperCase());
                    Date startTime = rs.getTimestamp("creationTime", utc);
                    String runID = rs.getString("runID");
                    Object ownerID = rs.getObject("ownerID");
                    Subject osub = identManager.toSubject(ownerID);
                    String odisp = identManager.toDisplayString(osub);
                    return new JobRef(rs.getString("jobID"), executionPhase, startTime, runID, odisp);
                }
            });

            if (!jobs.isEmpty()) {
                if (lastCreationTime != null) {
                    // check for duplicate due to creationTime <= ? constraint
                    // only need to check the first one because of the order by
                    JobRef jr = jobs.get(0);
                    if (lastCreationTime.equals(jr.getCreationTime())
                            && lastJobID.equals(jr.getJobID())) {
                        log.debug("iterator filter duplicate: " + jr);
                        jobs.remove(0);
                    }
                }
            }

            return jobs.iterator();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    // extract a single phase value from the result set
    private class PhaseExtractor implements ResultSetExtractor {

        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (rs.next()) {
                return ExecutionPhase.valueOf(getString(rs, jobSchema.jobTable, "executionPhase"));
            }
            return null;
        }

    }

    // map rows from the JobDetailSelectStatementCreator into Parameter or Result
    // and store them in the specified job
    private class DetailExtractor implements ResultSetExtractor {

        private JobSchema js;
        private Job job;

        public DetailExtractor(JobSchema js, Job job) {
            this.js = js;
            this.job = job;
            if (job.getParameterList() == null) {
                job.setParameterList(new ArrayList<Parameter>());
            }
            if (job.getResultsList() == null) {
                job.setResultsList(new ArrayList<Result>());
            }
        }

        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            while (rs.next()) {
                mapAndStoreRow(rs);
            }
            return job;
        }

        public void mapAndStoreRow(ResultSet rs) throws SQLException {
            String type = rs.getString("type");
            String name = rs.getString("name");
            String value = getString(rs, jobSchema.detailTable, "value");

            if (TYPE_PARAMETER.equals(type)) {
                job.getParameterList().add(new Parameter(name, value));
            } else if (TYPE_RESULT.equals(type)) {
                try {
                    URI uri = new URI(value);
                    job.getResultsList().add(new Result(name, uri));
                } catch (URISyntaxException ex) {
                    throw new IllegalStateException("failed to convert " + value + " to a URI");
                }
            } else {
                throw new IllegalStateException("unexpected type in param table: " + type);
            }
        }
    }

}
