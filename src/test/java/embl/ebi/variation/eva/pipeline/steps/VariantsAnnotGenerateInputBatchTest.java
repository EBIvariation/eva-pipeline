/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.steps;

import com.mongodb.*;
import com.mongodb.util.JSON;
import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantAnnotationItemProcessor;
import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @author Diego Poggioli
 *
 * Test {@link VariantsAnnotGenerateInputBatch}
 */
public class VariantsAnnotGenerateInputBatchTest {

    private MongoClient mongoClient;
    private String databaseName;
    private String collectionName = "dummy";

    private VariantsAnnotGenerateInputBatch variantsAnnotGenerateInputBatch;
    private ExecutionContext executionContext;

    // temporary output file
    private File outputFile;

    // reads the output file to check the result
    private BufferedReader reader;

    @Before
    public void setUp() throws Exception {
        databaseName = PropertiesLoaderUtils.loadAllProperties("application.properties").getProperty("mongo.db");
        mongoClient = new MongoClient();

        variantsAnnotGenerateInputBatch = new VariantsAnnotGenerateInputBatch();
        executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        outputFile = File.createTempFile("flatfile-test-output-", ".tmp");

        collection().drop();
    }

    /**
     * Release resources and delete the temporary output file
     */
    @After
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }

        outputFile.delete();
    }

    @Test
    public void variantReaderShouldReadVariantsWithoutAnnotationField() throws Exception {
        insertDocuments();
        MongoItemReader<DBObject> mongoItemReader = variantsAnnotGenerateInputBatch.initReader(collectionName, new MongoTemplate(mongoClient, databaseName));
        mongoItemReader.open(executionContext);

        int itemCount = 0;
        DBObject doc;
        while((doc = mongoItemReader.read()) != null) {
            itemCount++;
            assertTrue(doc.containsField("chr"));
            assertTrue(doc.containsField("start"));
            assertFalse(doc.containsField("annot"));
        }
        assertEquals(itemCount, 1);
        mongoItemReader.close();
    }

    @Test
    public void vepInputLineProcessorShouldConvertAllFieldsInVariant() throws Exception {
        DBObject dbo = constructDbo(variantWithoutAnnotation);

        ItemProcessor<DBObject, VariantWrapper> processor =  new VariantAnnotationItemProcessor();
        VariantWrapper variant = processor.process(dbo);
        assertEquals("+", variant.getStrand());
        assertEquals("20", variant.getChr());
        assertEquals("G/A", variant.getRefAlt());
        assertEquals(60343, variant.getEnd());
        assertEquals(60343, variant.getStart());
    }

    @Test
    public void vepInputWriterShouldWriteAllFieldsToFile() throws Exception {
        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        VariantWrapper variant = new VariantWrapper(converter.convertToDataModelType(constructDbo(variantWithAnnotation)));

        FlatFileItemWriter<VariantWrapper> writer = variantsAnnotGenerateInputBatch.initWriter(new FileSystemResource(outputFile));
        writer.open(executionContext);
        writer.write(Collections.singletonList(variant));
        assertEquals("20\t60344\t60348\tG/A\t+", readLine());
        writer.close();
    }


    private void insertDocuments() {
        collection().insert(constructDbo(variantWithAnnotation));
        collection().insert(constructDbo(variantWithoutAnnotation));
    }

    private DBCollection collection() {
        return mongoClient.getDB(databaseName).getCollection(collectionName);
    }

    private DBObject constructDbo(String variant) {
        return (DBObject) JSON.parse(variant);
    }

    /*
    * Read a line from the output file, if the reader has not been created,
    * recreate. This method is only necessary because running the tests in a
    * UNIX environment locks the file if it's open for writing.
    */
    private String readLine() throws IOException {
        if (reader == null) {
            reader = new BufferedReader(new FileReader(outputFile));
        }

        return reader.readLine();
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

    private final String variantWithAnnotation = "{\n" +
            "\t\"_id\" : \"20_60344_G_T\",\n" +
            "\t\"chr\" : \"20\",\n" +
            "\t\"start\" : 60344,\n" +
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
            "\t\"end\" : 60348,\n" +
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
            "\t],\n" +
            "\t\"annot\" : {\n" +
            "\t\t\"ct\" : [\n" +
            "\t\t\t{\n" +
            "\t\t\t\t\"so\" : [\n" +
            "\t\t\t\t\t1628\n" +
            "\t\t\t\t]\n" +
            "\t\t\t}\n" +
            "\t\t]\n" +
            "\t}\n" +
            "}";
}
