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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_ANNOTATE_VARIANTS_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link AnnotationJobConfiguration}
 * <p>
 * TODO The test should fail when we will integrate the JobParameter validation since there are empty parameters for VEP
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})

@ContextConfiguration(classes = {AnnotationJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class})
public class AnnotationJobTest extends MongoTestContainerHelper {
    private static final String MOCK_VEP = "/mockvep.pl";
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";
    private static final String INPUT_STUDY_ID = "1";
    private static final String INPUT_VCF_ID = "1";
    private static final String COLLECTION_ANNOTATIONS_NAME = "annotations";
    private static final String COLLECTION_ANNOTATION_METADATA_NAME = "annotationMetadata";
    private static final String COLLECTION_VARIANTS_NAME = "variants";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    private static final String DB_NAME = "annotate-variant-test-db";

    @Autowired
    @Qualifier(JOB_ANNOTATE_VARIANTS_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

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
    public void allAnnotationStepsShouldBeExecuted() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);
        String outputDirAnnot = temporaryFolderUtil.getRoot().getAbsolutePath();

        File vepOutput = new File(URLHelper.resolveVepOutput(outputDirAnnot, INPUT_STUDY_ID, INPUT_VCF_ID));
        String vepOutputName = vepOutput.getName();
        temporaryFolderUtil.newFile(vepOutputName);
        File fasta = temporaryFolderUtil.newFile();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .annotationOverwrite("false")
                .collectionAnnotationMetadataName(COLLECTION_ANNOTATION_METADATA_NAME)
                .collectionAnnotationsName(COLLECTION_ANNOTATIONS_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputFasta(fasta.getAbsolutePath())
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(outputDirAnnot)
                .vepCachePath("")
                .vepCacheSpecies("human")
                .vepCacheVersion("80")
                .vepNumForks("4")
                .vepPath(getResource(MOCK_VEP).getPath())
                .vepTimeout("60")
                .vepVersion("80")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertCompleted(jobExecution);

        assertEquals(2, jobExecution.getStepExecutions().size());
        List<StepExecution> steps = new ArrayList<>(jobExecution.getStepExecutions());
        StepExecution generateVepAnnotationsStep = steps.get(0);
        StepExecution loadAnnotationMetadataStep = steps.get(1);

        assertEquals(BeanNames.GENERATE_VEP_ANNOTATION_STEP, generateVepAnnotationsStep.getStepName());
        assertEquals(BeanNames.LOAD_ANNOTATION_METADATA_STEP, loadAnnotationMetadataStep.getStepName());

        //check that documents have the annotation
        MongoCursor<Document> cursor = mongoTemplate.getDb().getCollection(COLLECTION_ANNOTATIONS_NAME).find().iterator();

        int annotationCount = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            annotationCount++;
            Document annotation = cursor.next();
            List<Document> consequenceTypes = (List<Document>) annotation.get(CONSEQUENCE_TYPE_FIELD);
            assertNotNull(consequenceTypes);
            consequenceTypeCount += consequenceTypes.size();
        }
        cursor.close();

        assertEquals(300, annotationCount);
        assertEquals(537, consequenceTypeCount);
    }

}
