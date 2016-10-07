package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;

import static junit.framework.TestCase.assertTrue;

/**
 * @author Diego Poggioli
 *
 * Test for {@link PedLoaderStep}
 *
 * Using reflections to inject jobOptions into the tasklet. There are other ways to do this but a been is required.
 * Otherewise the tasklet needs to be into a Job. See:
 * http://docs.spring.io/spring-batch/reference/html/testing.html
 * 10.4 Testing Step-Scoped Components
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, CommonConfiguration.class})
public class PedLoaderStepTest {

    @Autowired
    private JobOptions jobOptions;

    private PedLoaderStep pedLoaderStep = new PedLoaderStep();

    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void allPedFileShouldBeParsedIntoPedigree() throws Exception {
        ReflectionTestUtils.setField(pedLoaderStep, "jobOptions", jobOptions);
        RepeatStatus status = pedLoaderStep.execute(stepContribution, chunkContext);

        assertTrue(status.equals(RepeatStatus.FINISHED));
    }

    @Before
    public void setUp() throws Exception {
        chunkContext = new ChunkContext(null);
        stepContribution = new StepContribution(new StepExecution("PedLoader", null));

        jobOptions.loadArgs();

        String pedigreeFile = PedLoaderStepTest.class.getResource("/integrated_call_samples.20101123.ped.txt").getFile();
        jobOptions.getPipelineOptions().put("input.pedigree", pedigreeFile);
    }

}
