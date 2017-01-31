/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * Test for {@link AggregatedVcfJob}
 */

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:variant-aggregated.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AggregatedVcfJob.class, BatchTestConfiguration.class})
public class AggregatedVcfJobTest {
    public static final String INPUT = "/aggregated.vcf.gz";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    public static final Set<String> EXPECTED_REQUIRED_STEP_NAMES = new TreeSet<>(
            Arrays.asList(BeanNames.LOAD_VARIANTS_STEP, BeanNames.LOAD_FILE_STEP));

    @Test
    public void aggregatedTransformAndLoadShouldBeExecuted() throws Exception {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName("variants")
                .databaseName(dbName)
                .inputStudyId("aggregated-job")
                .inputVcf(getResource(INPUT).getAbsolutePath())
                .inputVcfAggregation("BASIC")
                .inputVcfId("1")
                .timestamp()
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // check execution flow
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        Set<String> names = stepExecutions.stream().map(StepExecution::getStepName)
                .collect(Collectors.toSet());

        assertEquals(EXPECTED_REQUIRED_STEP_NAMES, names);

        StepExecution lastRequiredStep = new ArrayList<>(stepExecutions).get(EXPECTED_REQUIRED_STEP_NAMES.size() - 1);
        assertEquals(BeanNames.LOAD_FILE_STEP, lastRequiredStep.getStepName());

        // check ((documents in DB) == (lines in file))
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());

        File file = getResource(INPUT);
        long lines = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(file)));
        Assert.assertEquals(lines, JobTestUtils.count(iterator));

        // check that stats are loaded properly
        Variant variant = variantDBAdaptor.iterator(new QueryOptions()).next();
        assertFalse(variant.getSourceEntries().values().iterator().next().getCohortStats().isEmpty());
    }

// TODO This test needs to be refactored, as right the pipeline will handle the injection of the appropriate variant
// source even if the aggregated job has been selected.
//    @Test
//    public void aggregationNoneIsNotAllowed() throws Exception {
//        mongoRule.getTemporaryDatabase(dbName);
//        VariantSource source =
//                (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE);
//        jobOptions.getVariantOptions().put(
//                VariantStorageManager.VARIANT_SOURCE, new VariantSource(
//                        input,
//                        source.getFileId(),
//                        source.getStudyId(),
//                        source.getStudyName(),
//                        source.getType(),
//                        VariantSource.Aggregation.NONE));
//
//        Config.setOpenCGAHome(opencgaHome);
//
//        JobParameters jobParameters = new EvaJobParameterBuilder().inputVcf(getResource(input).getAbsolutePath())
//                .databaseName(dbName)
//                .collectionVariantsName("variants")
//                .inputVcfId("1")
//                .inputStudyId("aggregated-job")
//                .inputStudyName("studyName")
//                .inputStudyType("COLLECTION")
//                .inputVcfAggregation("NONE")
//                .timestamp().build();
//        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
//
//        assertEquals(ExitStatus.FAILED, jobExecution.getExitStatus());
//        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
//    }

}
