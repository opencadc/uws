
package ca.nrc.cadc.uws.server;

import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import ca.nrc.cadc.uws.ExecutionPhase;
/**
 *
 * @author adriand
 */
public class JobPersistenceUtilTest
{
    private static Logger log = Logger.getLogger(JobPersistenceUtilTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.uws", Level.INFO);
    }

    @Test
    public void testPhaseTransitions()
    {
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.PENDING, ExecutionPhase.QUEUED);
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING);
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.QUEUED, ExecutionPhase.PENDING);
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED);
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.EXECUTING, ExecutionPhase.ABORTED);
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED);
        JobPersistenceUtil.constraintPhaseTransition(ExecutionPhase.EXECUTING, ExecutionPhase.ERROR);
    }
}
