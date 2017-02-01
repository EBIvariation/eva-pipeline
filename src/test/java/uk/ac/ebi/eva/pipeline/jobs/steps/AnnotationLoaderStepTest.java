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

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.data.VepOutputContent;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.URLHelper;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;


/**
 * Test for {@link AnnotationLoaderStep}. In the context it is loaded {@link AnnotationJob}
 * because {@link JobLauncherTestUtils} require one {@link org.springframework.batch.core.Job} to be present in order
 * to run properly.
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:annotation-loader-step.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AnnotationJob.class, BatchTestConfiguration.class})
public class AnnotationLoaderStepTest {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

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
        String collectionVariantsName = "variants";
        String studyId = "1";
        String fileId = "1";
        String vepOutput = URLHelper.resolveVepOutput(annotationFolder, studyId, fileId);
        String vepOutputName = Paths.get(vepOutput).getFileName().toString();
        temporaryFolderRule.newGzipFile(VepOutputContent.vepOutputContent, vepOutputName);

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(collectionVariantsName)
                .databaseName(dbName)
                .inputStudyId(studyId)
                .inputVcfId(fileId)
                .outputDirAnnotation(annotationFolder)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VEP_ANNOTATION_STEP, jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //check that documents have the annotation
        DBCursor cursor = mongoRule.getCollection(dbName, collectionVariantsName).find();

        DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();

        int count = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject dbObject = (DBObject) cursor.next().get(VariantToDBObjectConverter.ANNOTATION_FIELD);
            if (dbObject != null) {
                VariantAnnotation annot = converter.convertToDataModelType(dbObject);
                Assert.assertNotNull(annot.getConsequenceTypes());
                consequenceTypeCount += annot.getConsequenceTypes().size();
            }
        }

        assertEquals(300, count);
        assertTrue("Annotations not found", consequenceTypeCount > 0);
    }

}
