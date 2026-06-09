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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.commons.mongodb.entities.VariantSourceMongo;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.GenotypedVcfJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_LOAD_VCF_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link LoadFileStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {GenotypedVcfJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class})
public class LoadFileStepTest extends MongoTestContainerHelper {

    private static final int EXPECTED_FILES = 1;

    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String DB_NAME = "load-file-step-test-db";

    @Autowired
    @Qualifier(JOB_LOAD_VCF_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

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
    public void loaderStepShouldLoadAllFiles() {
        String input = getResource(SMALL_VCF_FILE).getAbsolutePath();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName("variants")
                .databaseName(DB_NAME)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_FILE_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be equals to the number of VCF files loaded
        MongoCollection<Document> fileCollection = mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME);
        MongoCursor<Document> cursor = fileCollection.find().iterator();
        assertEquals(EXPECTED_FILES, count(cursor));
    }

    @Test
    public void loaderStepShouldLoadAllFiles_withExistingEntries() {
        File inputFile = getResource(SMALL_VCF_FILE);
        String databaseName = "file-step-test-db";

        // insert document with same file id, study id and filename as the one processed by the job
        mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME)
                .insertOne(new Document(VariantSourceMongo.FILEID_FIELD, "1")
                        .append(VariantSourceMongo.STUDYID_FIELD, "1")
                        .append(VariantSourceMongo.FILENAME_FIELD, inputFile.getName()));

        // run the job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName("variants")
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(inputFile.getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_FILE_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be equals to the number of VCF files loaded
        MongoCollection<Document> fileCollection = mongoTemplate.getDb().getCollection(COLLECTION_FILES_NAME);
        MongoCursor<Document> cursor = fileCollection.find().iterator();
        assertEquals(EXPECTED_FILES, count(cursor));
    }

}
