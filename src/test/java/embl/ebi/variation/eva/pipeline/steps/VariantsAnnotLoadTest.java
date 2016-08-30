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
package embl.ebi.variation.eva.pipeline.steps;

import com.mongodb.*;
import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationLineMapper;
import embl.ebi.variation.eva.pipeline.config.AnnotationConfig;
import embl.ebi.variation.eva.pipeline.jobs.*;
import embl.ebi.variation.eva.pipeline.steps.readers.VariantAnnotationReader;
import embl.ebi.variation.eva.pipeline.steps.writers.VariantAnnotationWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.makeGzipFile;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.restoreMongoDbFromDump;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;


/**
 * @author Diego Poggioli
 *
 * Test for {@link VariantsAnnotLoad}. In the context it is loaded {@link VariantAnnotConfiguration}
 * because {@link JobLauncherTestUtils} require one {@link org.springframework.batch.core.Job} to be present in order
 * to run properly.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantAnnotConfiguration.class, AnnotationConfig.class, JobLauncherTestUtils.class})
public class VariantsAnnotLoadTest {

    @Autowired private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired private VariantJobsArgs variantJobsArgs;

    private ExecutionContext executionContext;
    private String dbName;
    private DBObjectToVariantAnnotationConverter converter;
    private MongoClient mongoClient;

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
        dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        converter = new DBObjectToVariantAnnotationConverter();
        mongoClient = new MongoClient();
    }

    @Test
    public void variantAnnotLoadStepShouldLoadAllAnnotations() throws Exception {
        String dump = VariantsAnnotLoadTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump);

        String vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");
        makeGzipFile(vepOutputContent, vepOutput);

        JobExecution jobExecution = jobLauncherTestUtils.launchStep("Load VEP annotation");

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //check that documents have the annotation
        DBCursor cursor =
                collection(dbName, variantJobsArgs.getPipelineOptions().getString("db.collections.variants.name")).find();

        int cnt=0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            DBObject dbObject = (DBObject)cursor.next().get("annot");
            if(dbObject != null){
                VariantAnnotation annot = converter.convertToDataModelType(dbObject);
                Assert.assertNotNull(annot.getConsequenceTypes());
                consequenceTypeCount += annot.getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertTrue("Annotations not found", consequenceTypeCount>0);
    }

    @Test
    public void variantAnnotationReaderShouldReadAllFieldsInVepOutput() throws Exception {
        VariantAnnotationLineMapper lineMapper = new VariantAnnotationLineMapper();
        for (String annotLine : vepOutputContent.split("\n")) {
            VariantAnnotation variantAnnotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(variantAnnotation.getConsequenceTypes());
        }
    }

    @Test
    public void variantAnnotationReaderShouldReadAllLinesInVepOutput() throws Exception {
        String vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");

        //simulate VEP output file
        makeGzipFile(vepOutputContent, vepOutput);

        VariantAnnotationReader variantAnnotationReader = new VariantAnnotationReader(variantJobsArgs.getPipelineOptions());
        variantAnnotationReader.setSaveState(false);
        variantAnnotationReader.open(executionContext);

        VariantAnnotation variantAnnotation;
        int consequenceTypeCount = 0;
        int count = 0;
        while ((variantAnnotation = variantAnnotationReader.read()) != null) {
            count++;
            if (variantAnnotation.getConsequenceTypes() != null && !variantAnnotation.getConsequenceTypes().isEmpty()) {
                consequenceTypeCount++;
            }
        }
        // all should have at least consequence type annotations
        assertEquals(count, consequenceTypeCount);

        // variantAnnotationReader should get all the lines from the file
        long actualCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(vepOutput)));
        assertEquals(actualCount, count);
    }

    // Missing '/' in 20_63351_AG (sould be 20_63351_A/G)
    @Test(expected = FlatFileParseException.class)
    public void malformedVariantFieldsAnnotationLinesShouldBeSkipped() throws Exception {
        String vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");
        makeGzipFile(vepOutputContentMalformedVariantFields, vepOutput);
        VariantAnnotationReader variantAnnotationReader = new VariantAnnotationReader(variantJobsArgs.getPipelineOptions());
        variantAnnotationReader.open(executionContext);
        variantAnnotationReader.read();
    }

    // Missing ':' in 20_63351 (should be 20:63351)
    @Test(expected = FlatFileParseException.class)
    public void malformedCoordinatesAnnotationLinesShouldBeSkipped() throws Exception {
        String vepOutput = variantJobsArgs.getPipelineOptions().getString("vep.output");
        makeGzipFile(vepOutputContentMalformedCoordinates, vepOutput);
        VariantAnnotationReader variantAnnotationReader = new VariantAnnotationReader(variantJobsArgs.getPipelineOptions());
        variantAnnotationReader.open(executionContext);
        variantAnnotationReader.read();
    }

    @Test
    public void variantAnnotationWriterShouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbCollectionVariantsName = variantJobsArgs.getPipelineOptions().getString("db.collections.variants.name");
        JobTestUtils.cleanDBs(dbName);

        // first do a mock of a "variants" collection, with just the _id
        VariantAnnotationLineMapper lineMapper = new VariantAnnotationLineMapper();
        List<VariantAnnotation> annotations = new ArrayList<>();
        for (String annotLine : vepOutputContent.split("\n")) {
            annotations.add(lineMapper.mapLine(annotLine, 0));
        }
        MongoClient mongoClient = new MongoClient();
        DBCollection variants =
                mongoClient.getDB(dbName).getCollection(dbCollectionVariantsName);
        DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();

        Set<String> uniqueIdsLoaded = new HashSet<>();
        for (VariantAnnotation annotation : annotations) {
            String id = MongoDBHelper.buildStorageId(
                    annotation.getChromosome(),
                    annotation.getStart(),
                    annotation.getReferenceAllele(),
                    annotation.getAlternativeAllele());

            if (!uniqueIdsLoaded.contains(id)){
                variants.insert(new BasicDBObject("_id", id));
                uniqueIdsLoaded.add(id);
            }

        }

        // now, load the annotation
        VariantAnnotationWriter annotationWriter = new VariantAnnotationWriter(variantJobsArgs.getPipelineOptions());
        annotationWriter.write(annotations);

        // and finally check that documents in DB have annotation (only consequence type)
        DBCursor cursor = variants.find();

        int cnt=0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            VariantAnnotation annot = converter.convertToDataModelType((DBObject)cursor.next().get("annot"));
            assertTrue(annot.getConsequenceTypes() != null);
            consequenceTypeCount += annot.getConsequenceTypes().size();
        }
        assertTrue(cnt>0);
        assertEquals(annotations.size(), consequenceTypeCount);
    }

    /**
     * Release resources and delete the temporary output file
     */
    @After
    public void tearDown() throws Exception {
        mongoClient.close();
        JobTestUtils.cleanDBs(dbName);
    }

    private DBCollection collection(String databaseName, String collectionName) {
        return mongoClient.getDB(databaseName).getCollection(collectionName);
    }

    private final String vepOutputContent = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4991;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0014;AMR_MAF=T:0.01\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4531;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0014;AMR_MAF=T:0.01\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4952;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4492;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4925;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0028;AFR_MAF=T:0.01\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4465;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0028;AFR_MAF=T:0.01\n";

    private final String vepOutputContentMalformedVariantFields = "" +
            "20_63351_AG\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

    private final String vepOutputContentMalformedCoordinates = "" +
            "20_63351_A/G\t20_63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n";

}
