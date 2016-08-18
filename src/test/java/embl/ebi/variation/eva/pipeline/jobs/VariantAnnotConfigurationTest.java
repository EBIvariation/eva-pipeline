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

package embl.ebi.variation.eva.pipeline.jobs;

import com.mongodb.*;
import embl.ebi.variation.eva.VariantJobsArgs;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.restoreMongoDbFromDump;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantAnnotConfiguration}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantAnnotConfiguration.class, AnnotationConfig.class, JobLauncherTestUtils.class})
public class VariantAnnotConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    private static String dbName;
    private static MongoClient mongoClient;
    private File vepInputFile;
    private File vepOutputFile;
    private DBObjectToVariantAnnotationConverter converter;

    @Test
    public void fullAnnotationJob () throws Exception {
        String dump = VariantStatsConfigurationTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        if(vepInputFile.exists())
            vepInputFile.delete();

        assertFalse(vepInputFile.exists());

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //check list of variants without annotation output file
        assertTrue(vepInputFile.exists());
        assertEquals("20\t60343\t60343\tG/A\t+", readFirstLine(vepInputFile));

        //check that documents have the annotation
        DBCursor cursor =
                collection(dbName, variantJobsArgs.getPipelineOptions().getString("dbCollectionVariantsName")).find();

        int cnt=0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            DBObject dbObject = (DBObject)cursor.next().get("annot");
            if(dbObject != null){
                VariantAnnotation annot = converter.convertToDataModelType(dbObject);
                assertNotNull(annot.getConsequenceTypes());
                consequenceTypeCount += annot.getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertEquals(533, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals("variantAnnotLoadBatchStep"))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());
    }

    @Test
    public void annotCreateStepShouldGenerateAnnotations() throws Exception {

        String vepPath  = variantJobsArgs.getPipelineOptions().getString("vepPath");

        File vepPathFile =
                new File(VariantAnnotConfigurationTest.class.getResource("/mockvep.pl").getFile());
        //File tmpVepPathFile = new File(variantJobsArgs.getPipelineOptions().getString("outputDir"), vepPathFile.getName());
        //FileUtils.copyFile(vepPathFile, tmpVepPathFile);

        variantJobsArgs.getPipelineOptions().put("vepPath", vepPathFile);


        vepOutputFile.delete();
        TestCase.assertFalse(vepOutputFile.exists());  // ensure the annot file doesn't exist from previous executions

        // When the execute method in variantsAnnotCreate is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("annotationCreate");

        //Then variantsAnnotCreate step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And VEP output should exist and annotations should be in the file
        TestCase.assertTrue(vepOutputFile.exists());
        Assert.assertEquals(537, getLines(new GZIPInputStream(new FileInputStream(vepOutputFile))));
        vepOutputFile.delete();
    }

    private String readFirstLine(File file) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            return reader.readLine();
        }
    }

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        vepInputFile = new File(variantJobsArgs.getPipelineOptions().getString("vepInput"));
        vepOutputFile = new File(variantJobsArgs.getPipelineOptions().getString("vepOutput"));
        converter = new DBObjectToVariantAnnotationConverter();

        dbName = variantJobsArgs.getPipelineOptions().getString(VariantStorageManager.DB_NAME);
        mongoClient = new MongoClient();
    }

    /**
     * Release resources and delete the temporary output file
     */
    @After
    public void tearDown() throws Exception {
        mongoClient.close();

        vepInputFile.delete();
        new File(variantJobsArgs.getPipelineOptions().getString("vepOutput")).delete();

        JobTestUtils.cleanDBs(dbName);
    }

    private DBCollection collection(String databaseName, String collectionName) {
        return mongoClient.getDB(databaseName).getCollection(collectionName);
    }

}