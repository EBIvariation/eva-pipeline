package uk.ac.ebi.eva.pipeline;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * The purpose of this test is to imitate an execution made by an user through the CLI.
 * This is needed because all the other tests just instantiate what they need (just one step, or just one job) and
 * sometimes we have errors due to collisions instantiating several jobs. This test should instantiate everything
 * Spring instantiates in a real execution.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"integrationTest,test,mongo"})
public class ApplicationTest {

    @Test
    public void main() throws Exception {
    }

}
