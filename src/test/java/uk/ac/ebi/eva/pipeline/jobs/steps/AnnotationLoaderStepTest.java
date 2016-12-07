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
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationLoaderStepTestConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.test.data.VepOutputContent;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.makeGzipFile;


/**
 * Test for {@link AnnotationLoaderStep}. In the context it is loaded {@link AnnotationJob}
 * because {@link JobLauncherTestUtils} require one {@link org.springframework.batch.core.Job} to be present in order
 * to run properly.
 */
@RunWith(SpringRunner.class)
@ActiveProfiles("variant-annotation-mongo")
@ContextConfiguration(classes = {AnnotationJob.class, AnnotationLoaderStepTestConfiguration.class, JobLauncherTestUtils.class})
public class AnnotationLoaderStepTest {
    // TODO vep Output must be passed as a job parameter to allow temporary files. Database name can't be changed to a
    // random one.

    private static final String DATABASE_NAME = AnnotationLoaderStepTest.class.getSimpleName();
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private JobOptions jobOptions;

    @Test
    public void shouldLoadAllAnnotations() throws Exception {
        setUp();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(AnnotationLoaderStep.LOAD_VEP_ANNOTATION);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //check that documents have the annotation
        DBCursor cursor = mongoRule.getCollection(jobOptions.getDbName(), jobOptions.getDbCollectionsVariantsName())
                .find();

        DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();

        int cnt = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            DBObject dbObject = (DBObject) cursor.next().get("annot");
            if (dbObject != null) {
                VariantAnnotation annot = converter.convertToDataModelType(dbObject);
                Assert.assertNotNull(annot.getConsequenceTypes());
                consequenceTypeCount += annot.getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertTrue("Annotations not found", consequenceTypeCount > 0);
    }

    private void setUp() throws Exception {
        jobOptions.loadArgs();
        jobOptions.setDbName(DATABASE_NAME);

        mongoRule.restoreDump(getResourceUrl(MONGO_DUMP), jobOptions.getDbName());

        //TODO change for commented lines when vep output file can be passed as a job parameter
        //File file = temporaryFolderRule.newGzipFile(VepOutputContent.vepOutputContent);
        //jobOptions.setVepOutput(file.getAbsolutePath());
        File file = makeGzipFile(VepOutputContent.vepOutputContent, jobOptions.getVepOutput());

    }

}
