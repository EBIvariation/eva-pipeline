/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Diego Poggioli
 *         <p>
 *         Test for {@link PedLoaderStep}
 *         <p>
 *         Using reflections to inject jobOptions into the tasklet. There are other ways to do this but a been is required.
 *         <p>
 *         TODO: This could be simplified by using StepRunner. That requires using JobParametersinstead of our ownJobOptions
 *         http://docs.spring.io/spring-batch/apidocs/org/springframework/batch/test/StepRunner.html
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {JobOptions.class, CommonConfiguration.class})
public class PedLoaderStepTest {

    @Autowired
    private JobOptions jobOptions;

    private PedLoaderStep pedLoaderStep;
    private ChunkContext chunkContext;
    private StepContribution stepContribution;

    @Test
    public void allPedFileShouldBeParsedIntoPedigree() throws Exception {
        String pedigreeFile = PedLoaderStepTest.class.getResource("/ped/pedigree-test-file.ped").getFile();
        jobOptions.getPipelineOptions().put("input.pedigree", pedigreeFile);

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

    @Test(expected = IllegalArgumentException.class)
    public void missingLastColumnInPedFileShouldThrowsException() throws Exception {
        String pedigreeFile = PedLoaderStepTest.class.getResource("/ped/malformed-pedigree-test-file.ped").getFile();
        jobOptions.getPipelineOptions().put("input.pedigree", pedigreeFile);

        ReflectionTestUtils.setField(pedLoaderStep, "jobOptions", jobOptions);
        pedLoaderStep.execute(stepContribution, chunkContext);
    }

    @Before
    public void setUp() throws Exception {
        pedLoaderStep = new PedLoaderStep();
        chunkContext = new ChunkContext(null);
        stepContribution = new StepContribution(new StepExecution("PedLoader", null));

        jobOptions.loadArgs();
    }

}
