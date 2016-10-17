package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.pedigree.Condition;
import org.opencb.biodata.models.pedigree.Individual;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.opencb.biodata.models.pedigree.Sex;
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

import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * @author Diego Poggioli
 *
 * Test for {@link PedLoaderStep}
 *
 * Using reflections to inject jobOptions into the tasklet. There are other ways to do this but a been is required.
 *
 * TODO: This could be simplified by using StepRunner. That requires using JobParametersinstead of our ownJobOptions
 * http://docs.spring.io/spring-batch/apidocs/org/springframework/batch/test/StepRunner.html
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

        Pedigree pedigree = pedLoaderStep.getPedigree();

        assertTrue(status.equals(RepeatStatus.FINISHED));

        //check that Pedigree.Individuals is correctly populated
        assertEquals(4, pedigree.getIndividuals().size());
        Individual individualNA19660 = pedigree.getIndividuals().get("NA19660");
        assertTrue(individualNA19660.getFamily().equals("FAM"));
        assertTrue(individualNA19660.getSex().equals("2"));
        assertEquals(Sex.FEMALE, individualNA19660.getSexCode());
        assertTrue(individualNA19660.getPhenotype().equals("1"));
        assertEquals(Condition.UNAFFECTED, individualNA19660.getCondition());
        assertEquals(2, individualNA19660.getChildren().size());
        assertEquals(Sets.newHashSet("NA19600", "NA19685"),
                individualNA19660.getChildren().stream().map(Individual::getId).collect(Collectors.toSet()));

        //check that Pedigree.Families is correctly populated
        assertEquals(1, pedigree.getFamilies().size());
        assertTrue(pedigree.getFamilies().containsKey("FAM"));
        assertEquals(4, pedigree.getFamilies().get("FAM").size());
    }

    @Before
    public void setUp() throws Exception {
        chunkContext = new ChunkContext(null);
        stepContribution = new StepContribution(new StepExecution("PedLoader", null));

        jobOptions.loadArgs();

        String pedigreeFile = PedLoaderStepTest.class.getResource("/ped/pedigree-test-file.ped").getFile();
        jobOptions.getPipelineOptions().put("input.pedigree", pedigreeFile);
    }

}
