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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.AnnotationJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.data.VepOutputContent;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.mongo.entity.Annotation.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.Annotation.XREFS_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.ConsequenceType.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.SO_ACCESSION_FIELD;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link LoadVepAnnotationStepConfiguration}. In the context it is loaded {@link AnnotationJobConfiguration}
 * because {@link JobLauncherTestUtils} require one {@link org.springframework.batch.core.Job} to be present in order
 * to run properly.
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AnnotationJobConfiguration.class, BatchTestConfiguration.class})
public class LoadVepAnnotationStepTest {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String COLLECTION_ANNOTATIONS_NAME = "annotations";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String INPUT_STUDY_ID = "1";

    private static final String INPUT_VCF_ID = "1";

    private static final String VEP_VERSION = "1";

    private static final String VEP_CACHE_VERSION = "1";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void shouldLoadAllAnnotations() throws Exception {
        String annotationFolder = temporaryFolderRule.getRoot().getAbsolutePath();
        String dbName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));
        String vepOutput = URLHelper.resolveVepOutput(annotationFolder, INPUT_STUDY_ID, INPUT_VCF_ID);
        String vepOutputName = Paths.get(vepOutput).getFileName().toString();
        temporaryFolderRule.newGzipFile(VepOutputContent.vepOutputContent, vepOutputName);

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .collectionAnnotationsName("annotations")
                .databaseName(dbName)
                .inputStudyId(INPUT_STUDY_ID)
                .inputVcfId(INPUT_VCF_ID)
                .outputDirAnnotation(annotationFolder)
                .vepCacheVersion(VEP_CACHE_VERSION)
                .vepVersion(VEP_VERSION)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VEP_ANNOTATION_STEP, jobParameters);

        assertCompleted(jobExecution);

        //check that the annotation collection has been populated properly
        DBCursor annotationCursor = mongoRule.getCollection(dbName, COLLECTION_ANNOTATIONS_NAME).find();

        int annotationCount = 0;
        int consequenceTypeCount = 0;
        while (annotationCursor.hasNext()) {
            annotationCount++;
            DBObject dbObject = annotationCursor.next();
            if (dbObject != null) {
                BasicDBList consequenceTypes = ((BasicDBList) dbObject.get(CONSEQUENCE_TYPE_FIELD));
                Assert.assertNotNull(consequenceTypes);
                consequenceTypeCount += consequenceTypes.size();
            }
        }
        annotationCursor.close();

        assertTrue("Annotations not found", annotationCount == 4);
        assertTrue("ConsequenceType not found", consequenceTypeCount == 7);

        //check that the annotation fields are present in the variant
        DBCursor variantCursor = mongoRule.getCollection(dbName, COLLECTION_VARIANTS_NAME).find();
        while (variantCursor.hasNext()) {
            DBObject variant = variantCursor.next();
            if (variant.get("_id").equals("20_63351_A_G")) {
                BasicDBObject annotationField = (BasicDBObject) ((BasicDBList) (variant).get(
                        VariantDocument.ANNOTATION_FIELD)).get(0);
                assertNotNull(annotationField.get(SIFT_FIELD));
                assertNotNull(annotationField.get(SO_ACCESSION_FIELD));
                assertNotNull(annotationField.get(POLYPHEN_FIELD));
                assertNotNull(annotationField.get(XREFS_FIELD));
            }
        }
        variantCursor.close();
    }

}
