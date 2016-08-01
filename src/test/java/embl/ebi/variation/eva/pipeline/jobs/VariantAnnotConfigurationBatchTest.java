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

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import embl.ebi.variation.eva.VariantJobsArgs;
import junit.framework.TestCase;
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
import java.net.UnknownHostException;
import java.util.zip.GZIPOutputStream;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantAnnotConfigurationBatch}
 */
@IntegrationTest
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantAnnotConfigurationBatch.class, AnnotationConfig.class})
public class VariantAnnotConfigurationBatchTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    private static String dbName;
    private static MongoClient mongoClient;
    private BufferedReader reader;
    private File vepInputFile;
    private DBObjectToVariantAnnotationConverter converter;

    @Test
    public void validAnnotationJob () throws Exception {
        variantJobsArgs.loadArgs();

        insertVariantsWithoutAnnotations(dbName, dbName);

        if(vepInputFile.exists())
            vepInputFile.delete();

        assertFalse(vepInputFile.exists());

        makeVepAnnotationTmpGzipFile();

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
            VariantAnnotation annot = converter.convertToDataModelType((DBObject)cursor.next().get("annot"));
            TestCase.assertTrue(annot.getConsequenceTypes() != null);
            consequenceTypeCount += annot.getConsequenceTypes().size();
        }

        assertTrue(cnt>0);
        assertEquals(1, consequenceTypeCount);
    }

    /**
     * This simulate VEP prediction
     *
     * @throws IOException
     */
    private void makeVepAnnotationTmpGzipFile() throws IOException {
        String vepAnnotation =
                "20_60343_G/A\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

        try(FileOutputStream output = new FileOutputStream(variantJobsArgs.getPipelineOptions().getString("vepOutput"))) {
            try(Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(vepAnnotation);
            }
        }

    }

    private String readLine(File outputFile) throws IOException {
        if (reader == null) {
            reader = new BufferedReader(new FileReader(outputFile));
        }
        return reader.readLine();
    }

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        vepInputFile = new File(variantJobsArgs.getPipelineOptions().getString("vepInput"));
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
        if (reader != null) {
            reader.close();
        }

        mongoClient.close();

        vepInputFile.delete();
        new File(variantJobsArgs.getPipelineOptions().getString("vepOutput")).delete();
    }

    private static void cleanDBs() throws UnknownHostException {
        JobTestUtils.cleanDBs(
                dbName
        );
    }

    private void insertVariantsWithoutAnnotations(String databaseName, String collectionName) {
        collection(databaseName, collectionName).insert(constructDbo(variantWithoutAnnotation));
    }

    private DBCollection collection(String databaseName, String collectionName) {
        return mongoClient.getDB(databaseName).getCollection(collectionName);
    }

    private DBObject constructDbo(String variant) {
        return (DBObject) JSON.parse(variant);
    }

    private final String variantWithoutAnnotation = "{\n" +
            "\t\"_id\" : \"20_60343_G_A\",\n" +
            "\t\"chr\" : \"20\",\n" +
            "\t\"start\" : 60343,\n" +
            "\t\"files\" : [\n" +
            "\t\t{\n" +
            "\t\t\t\"fid\" : \"5\",\n" +
            "\t\t\t\"sid\" : \"7\",\n" +
            "\t\t\t\"attrs\" : {\n" +
            "\t\t\t\t\"QUAL\" : \"100.0\",\n" +
            "\t\t\t\t\"FILTER\" : \"PASS\",\n" +
            "\t\t\t\t\"AC\" : \"1\",\n" +
            "\t\t\t\t\"AF\" : \"0.000199681\",\n" +
            "\t\t\t\t\"AN\" : \"5008\",\n" +
            "\t\t\t\t\"NS\" : \"2504\",\n" +
            "\t\t\t\t\"DP\" : \"0\",\n" +
            "\t\t\t\t\"ASN_AF\" : \"0.0000\",\n" +
            "\t\t\t\t\"AMR_AF\" : \"0.0014\",\n" +
            "\t\t\t\t\"AFR_AF\" : \"0.0000\",\n" +
            "\t\t\t\t\"EUR_AF\" : \"0.0000\",\n" +
            "\t\t\t\t\"SAN_AF\" : \"0.0000\",\n" +
            "\t\t\t\t\"ssID\" : \"ss1363765667\",\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t],\n" +
            "\t\"ids\" : [\n" +
            "\t\t\"rs527639301\"\n" +
            "\t],\n" +
            "\t\"type\" : \"SNV\",\n" +
            "\t\"end\" : 60343,\n" +
            "\t\"len\" : 1,\n" +
            "\t\"ref\" : \"G\",\n" +
            "\t\"alt\" : \"A\",\n" +
            "\t\"_at\" : {\n" +
            "\t\t\"chunkIds\" : [\n" +
            "\t\t\t\"20_60_1k\",\n" +
            "\t\t\t\"20_6_10k\"\n" +
            "\t\t]\n" +
            "\t},\n" +
            "\t\"hgvs\" : [\n" +
            "\t\t{\n" +
            "\t\t\t\"type\" : \"genomic\",\n" +
            "\t\t\t\"name\" : \"20:g.60343G>A\"\n" +
            "\t\t}\n" +
            "\t],\n" +
            "\t\"st\" : [\n" +
            "\t\t{\n" +
            "\t\t\t\"maf\" : -1,\n" +
            "\t\t\t\"mgf\" : -1,\n" +
            "\t\t\t\"mafAl\" : null,\n" +
            "\t\t\t\"mgfGt\" : null,\n" +
            "\t\t\t\"missAl\" : 0,\n" +
            "\t\t\t\"missGt\" : 0,\n" +
            "\t\t\t\"numGt\" : {\n" +
            "\t\t\t\t\n" +
            "\t\t\t},\n" +
            "\t\t\t\"cid\" : \"ALL\",\n" +
            "\t\t\t\"sid\" : \"7\",\n" +
            "\t\t\t\"fid\" : \"5\"\n" +
            "\t\t}\n" +
            "\t]\n" +
            "}";

}