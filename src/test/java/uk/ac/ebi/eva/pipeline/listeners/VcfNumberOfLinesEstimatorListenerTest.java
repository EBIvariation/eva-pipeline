/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.listeners;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link VcfNumberOfLinesEstimatorListener}
 */
public class VcfNumberOfLinesEstimatorListenerTest {

    private static final String GENOTYPED_VCF = "/input-files/vcf/genotyped.vcf.gz";

    private static final String AGGREGATED_VCF = "/input-files/vcf/aggregated.vcf.gz";

    private static final String SMALL_VCF = "/input-files/vcf/small_genotyped.vcf.gz";

    private static final int PERCENTAGE_SIMILARITY = 20;

    private VcfNumberOfLinesEstimatorListener estimatorListener;

    private Map<String, JobParameter> parameters;

    @Before
    public void setUp() throws Exception {
        estimatorListener = new VcfNumberOfLinesEstimatorListener();
        parameters = new HashMap<>();
    }

    @Test
    public void predictedGenotypedVcfNumberOfLines() {
        String genotypedVcfPath = getResource(GENOTYPED_VCF).getAbsolutePath();

        parameters.put(JobParametersNames.INPUT_VCF, new JobParameter(genotypedVcfPath));
        JobParameters jobParameters = new JobParameters(parameters);
        StepExecution stepExecution = new StepExecution("NoProcessingStep",
                                                        new JobExecution(new JobInstance(1L, "NoProcessingJob"), 1L,
                                                                         jobParameters, "NoProcessingConfiguration"));

        estimatorListener.beforeStep(stepExecution);

        int expectedNumberOfLines = 298;
        assertEquals(expectedNumberOfLines,
                     stepExecution.getExecutionContext().getInt(JobParametersNames.NUMBER_OF_LINES),
                     (expectedNumberOfLines / 100) * PERCENTAGE_SIMILARITY);
    }

    @Test
    public void predictedAggregatedVcfNumberOfLines() {
        String genotypedVcfPath = getResource(AGGREGATED_VCF).getAbsolutePath();

        parameters.put(JobParametersNames.INPUT_VCF, new JobParameter(genotypedVcfPath));
        JobParameters jobParameters = new JobParameters(parameters);
        StepExecution stepExecution = new StepExecution("NoProcessingStep",
                                                        new JobExecution(new JobInstance(1L, "NoProcessingJob"), 1L,
                                                                         jobParameters, "NoProcessingConfiguration"));

        estimatorListener.beforeStep(stepExecution);

        int expectedNumberOfLines = 156;
        assertEquals(expectedNumberOfLines,
                     stepExecution.getExecutionContext().getInt(JobParametersNames.NUMBER_OF_LINES),
                     (expectedNumberOfLines / 100) * PERCENTAGE_SIMILARITY);
    }

    @Test
    public void smallVcfNumberOfLines() {
        String genotypedVcfPath = getResource(SMALL_VCF).getAbsolutePath();

        parameters.put(JobParametersNames.INPUT_VCF, new JobParameter(genotypedVcfPath));
        JobParameters jobParameters = new JobParameters(parameters);
        StepExecution stepExecution = new StepExecution("NoProcessingStep",
                                                        new JobExecution(new JobInstance(1L, "NoProcessingJob"), 1L,
                                                                         jobParameters, "NoProcessingConfiguration"));

        estimatorListener.beforeStep(stepExecution);

        assertEquals(0, stepExecution.getExecutionContext().getInt(JobParametersNames.NUMBER_OF_LINES));
    }
}
