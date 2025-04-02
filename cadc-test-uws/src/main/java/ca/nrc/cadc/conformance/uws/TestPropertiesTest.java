
package ca.nrc.cadc.conformance.uws;

import java.io.File;
import java.io.FileReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPropertiesTest {

    TestProperties props;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        File file = new File("/home/cadc/zhangsa/work/develop/caom_ws/test/src/resources/ResultsTest-2.properties");
        props = new TestProperties();
        FileReader reader = new FileReader(file);
        props.load(reader, file.getName());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testTestProperties() {
        System.out.println(props.toString());
    }

}
