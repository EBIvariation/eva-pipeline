package uk.ac.ebi.eva.pipeline;

import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

/**
 * The purpose of this test is to imitate an execution made by an user through the CLI.
 * This is needed because all the other tests just instantiate what they need (just one step, or just one job) and
 * sometimes we have errors due to collisions instantiating several jobs. This test should instantiate everything
 * Spring instantiates in a real execution.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"integrationTest,test,mongo"})
@TestPropertySource({"classpath:test-mongo.properties"})
public class ApplicationTest {

    @Autowired
    JobExplorer jobExplorer;

    @Autowired
    JobOptions jobOptions;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Test
    public void main() throws Exception {
        mongoRule.getTemporaryDatabase(jobOptions.getDbName());

        Assert.assertEquals(1, jobExplorer.getJobNames().size());
        Assert.assertEquals(BeanNames.GENOTYPED_VCF_JOB, jobExplorer.getJobNames().get(0));

        List<JobInstance> jobInstances = jobExplorer.getJobInstances(BeanNames.GENOTYPED_VCF_JOB, 0, 100);
        Assert.assertEquals(1, jobInstances.size());

        JobExecution jobExecution = jobExplorer.getJobExecution(jobInstances.get(0).getInstanceId());
        Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
    }
}
