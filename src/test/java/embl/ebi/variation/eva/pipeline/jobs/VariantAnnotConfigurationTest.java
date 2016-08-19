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
import com.mongodb.util.JSON;
import embl.ebi.variation.eva.VariantJobsArgs;
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
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.makeGzipFile;
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
    private DBObjectToVariantAnnotationConverter converter;

    @Test
    public void fullAnnotationJob () throws Exception {

        insertVariantsWithoutAnnotations(dbName, dbName);

        if(vepInputFile.exists())
            vepInputFile.delete();

        assertFalse(vepInputFile.exists());

        //Simulate VEP prediction file (second line is malformed and it should be skipped)
        String vepAnnotation =
                "20_60343_G/A\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n"
                +"20_60344_GA\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

        makeGzipFile(vepAnnotation, variantJobsArgs.getPipelineOptions().getString("vep.output"));

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //check list of variants without annotation output file
        assertTrue(vepInputFile.exists());
        assertEquals("20\t60343\t60343\tG/A\t+", readLine(vepInputFile));

        //check that documents have the annotation
        DBCursor cursor = collection(dbName, dbName).find();

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

        assertTrue(cnt>0);
        assertEquals(1, consequenceTypeCount);

        //check that one line is skipped because malformed
        List<StepExecution> variantAnnotationLoadStepExecution = jobExecution.getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals("Load VEP annotation"))
                .collect(Collectors.toList());
        assertEquals(1, variantAnnotationLoadStepExecution.get(0).getReadSkipCount());
    }

    private String readLine(File outputFile) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(outputFile))){
            return reader.readLine();
        }
    }

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        vepInputFile = new File(variantJobsArgs.getPipelineOptions().getString("vep.input"));
        converter = new DBObjectToVariantAnnotationConverter();

        dbName = variantJobsArgs.getPipelineOptions().getString(VariantStorageManager.DB_NAME);
        mongoClient = new MongoClient();

        cleanDBs();
    }

    @AfterClass
    public static void afterTests() throws UnknownHostException {
        cleanDBs();
    }

    /**
     * Release resources and delete the temporary output file
     */
    @After
    public void tearDown() throws Exception {
        mongoClient.close();

        vepInputFile.delete();
        new File(variantJobsArgs.getPipelineOptions().getString("vep.output")).delete();
    }

    private static void cleanDBs() throws UnknownHostException {
        JobTestUtils.cleanDBs(
                dbName
        );
    }

    private void insertVariantsWithoutAnnotations(String databaseName, String collectionName) throws IOException {
        URL variantWithNoAnnotationUrl =
                VariantAnnotConfigurationTest.class.getResource("/annotation/VariantWithOutAnnotation");
        String variantWithoutAnnotation = FileUtils.readFileToString(new File(variantWithNoAnnotationUrl.getFile()));
        collection(databaseName, collectionName).insert(constructDbo(variantWithoutAnnotation));
    }

    private DBCollection collection(String databaseName, String collectionName) {
        return mongoClient.getDB(databaseName).getCollection(collectionName);
    }

    private DBObject constructDbo(String variant) {
        return (DBObject) JSON.parse(variant);
    }

}