/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.jobs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
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
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
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
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link LoadVcfJobConfiguration}
 */

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:variant-aggregated.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {LoadVcfJobConfiguration.class, BatchTestConfiguration.class,
        TemporaryRuleConfiguration.class, MongoCollectionNameConfiguration.class})
public class LoadVcfJobTest {
    public static final String INPUT = "/input-files/vcf/aggregated.vcf.gz";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String COLLECTION_FILES_NAME = "files";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    public static final Set<String> EXPECTED_REQUIRED_STEP_NAMES = new TreeSet<>(
            Arrays.asList(BeanNames.LOAD_VARIANTS_STEP, BeanNames.LOAD_FILE_STEP));

    @Before
    public void setUp() throws Exception {
        Config.setOpenCGAHome(GenotypedVcfJobTestUtils.getDefaultOpencgaHome());
    }

    @Test
    public void aggregatedLoadVcf() throws Exception {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(dbName)
                .inputStudyId("aggregated-job")
                .inputStudyName("inputStudyName")
                .inputStudyType("COLLECTION")
                .inputVcf(getResource(INPUT).getAbsolutePath())
                .inputAssemblyReport(GenotypedVcfJobTestUtils.getAssemblyReport())
                .inputVcfAggregation("BASIC")
                .inputVcfId("1")
                .timestamp()
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // check execution flow
        assertCompleted(jobExecution);

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

    @Test
    public void GenotypedLoadVcfJob() throws Exception {
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File fasta = temporaryFolderRule.newFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();

        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME)
                .collectionVariantsName(GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyName("inputStudyName")
                .inputStudyType("COLLECTION")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertCompleted(jobExecution);

        GenotypedVcfJobTestUtils.checkLoadStep(mongoRule, databaseName);

    }

}
