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

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_LOAD_VCF_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link LoadVcfJobConfiguration}
 */

@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {LoadVcfJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class})
public class LoadVcfJobTest extends MongoTestContainerHelper {
    public static final String INPUT = "/input-files/vcf/aggregated.vcf.gz";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String DB_NAME = "load-vcf-test-db";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Autowired
    @Qualifier(JOB_LOAD_VCF_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

    public static final Set<String> EXPECTED_REQUIRED_STEP_NAMES = new TreeSet<>(
            Arrays.asList(BeanNames.LOAD_VARIANTS_STEP, BeanNames.LOAD_FILE_STEP));

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate = batchTestConfiguration.getMongoTemplate(DB_NAME, mongoMappingContext);
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void aggregatedLoadVcf() throws Exception {
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputStudyId("aggregated-job")
                .inputStudyName("Test study name with non printable characters \u00e2\u0080\u0093in the name")
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
        MongoCursor<Document> iterator = mongoTemplate.getCollection(COLLECTION_VARIANTS_NAME).find().iterator();

        File file = getResource(INPUT);
        long lines = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(file)));
        assertEquals(lines, JobTestUtils.count(iterator));

        // check that stats are loaded properly
        Document variant = mongoTemplate.getCollection(COLLECTION_VARIANTS_NAME).find().iterator().next();
        List<Document> st = variant.getList("st", Document.class);
        assertNotNull(st);
        assertFalse(st.isEmpty());
    }

    @Test
    public void GenotypedLoadVcfJob() throws Exception {
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        File fasta = temporaryFolderUtil.newFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();

        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(GenotypedVcfJobTestUtils.COLLECTION_FILES_NAME)
                .collectionVariantsName(GenotypedVcfJobTestUtils.COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
                .inputStudyId(GenotypedVcfJobTestUtils.INPUT_STUDY_ID)
                .inputStudyName("Test study name with non printable characters \u00e2\u0080\u0093in the name")
                .inputStudyType("COLLECTION")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId(GenotypedVcfJobTestUtils.INPUT_VCF_ID)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertCompleted(jobExecution);

        GenotypedVcfJobTestUtils.checkLoadStep(mongoTemplate);

    }

}
