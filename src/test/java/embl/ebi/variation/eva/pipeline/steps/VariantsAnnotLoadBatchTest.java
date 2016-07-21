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
import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationLineMapper;
import embl.ebi.variation.eva.pipeline.jobs.JobTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;


/**
 * @author Diego Poggioli
 *
 * Test {@link VariantsAnnotLoadBatch}
 */
public class VariantsAnnotLoadBatchTest {


    private VariantsAnnotLoadBatch variantsAnnotLoadBatch;
    private ExecutionContext executionContext;

    @Before
    public void setUp() throws Exception {
        executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        variantsAnnotLoadBatch = new VariantsAnnotLoadBatch();
    }

    @Test
    public void variantAnnotationReaderShouldReadAllFieldsInVepOutput() throws Exception {
        VariantAnnotationLineMapper lineMapper = new VariantAnnotationLineMapper();
        for (String annotLine : vepOutput.split("\n")) {
            VariantAnnotation variantAnnotation = lineMapper.mapLine(annotLine, 0);
            assertTrue(variantAnnotation.getConsequenceTypes() != null);
        }
    }

    @Test
    public void variantAnnotationReaderShouldReadAllLinesInVepOutput() throws Exception {
        String vepOutput = VariantsAnnotLoadBatchTest.class.getResource("/annot.tsv.gz").getFile();

        FlatFileItemReader<VariantAnnotation> reader = variantsAnnotLoadBatch.initReader(new GzipLazyResource(vepOutput));

        reader.setSaveState(false);
        reader.open(executionContext);

        VariantAnnotation variantAnnotation;
        int consequenceTypeCount = 0;
//        int otherCount = 0;
        int count = 0;
        while ((variantAnnotation = reader.read()) != null) {
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

    @Test
    public void variantAnnotationWriterShouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbName = "VariantAnnotLoadBatchTest_writer";
        JobTestUtils.cleanDBs(dbName);

        // first do a mock of a "variants" collection, with just the _id
        VariantAnnotationLineMapper lineMapper = new VariantAnnotationLineMapper();
        List<VariantAnnotation> annotations = new ArrayList<>();
        for (String annotLine : vepOutput.split("\n")) {
            annotations.add(lineMapper.mapLine(annotLine, 0));
        }
        MongoClient mongoClient = new MongoClient();
        DBCollection variants = mongoClient.getDB(dbName).getCollection("variants");
        DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();
        for (VariantAnnotation annotation : annotations) {
            String id = buildStorageId(annotation.getChromosome(), annotation.getStart(),
                    annotation.getReferenceAllele(), annotation.getAlternativeAllele());
            try {
                variants.insert(new BasicDBObject("_id", id));
            } catch (MongoException.DuplicateKey e) {
                ; // ignore, we are just scaffolding the variants collection
            }
        }

        // now, load the annotation
        MongoOperations mongoOperations = new MongoTemplate(new MongoClient(), dbName);
        MongoItemWriter<VariantAnnotation> writer = variantsAnnotLoadBatch.initWriter("variants", mongoOperations);

        writer.write(annotations);

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

        JobTestUtils.cleanDBs(dbName);
    }

    private String buildStorageId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome);
        builder.append("_");
        builder.append(start);
        builder.append("_");
        if(!reference.equals("-")) {
            if(reference.length() < 50) {
                builder.append(reference);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(reference)));
            }
        }

        builder.append("_");
        if(!alternate.equals("-")) {
            if(alternate.length() < 50) {
                builder.append(alternate);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(alternate)));
            }
        }

        return builder.toString();
    }

    private final String vepOutput = "" +
            "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4991;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0014;AMR_MAF=T:0.01\n" +
            "20_63360_C/T\t20:63360\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs186156309\tDISTANCE=4531;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0014;AMR_MAF=T:0.01\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4952;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE\n" +
            "20_63399_G/A\t20:63399\tA\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\t-\tDISTANCE=4492;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000382410\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4925;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE;GMAF=T:0.0028;AFR_MAF=T:0.01\n" +
            "20_63426_G/T\t20:63426\tT\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs147063585\tDISTANCE=4465;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=T:0.0028;AFR_MAF=T:0.01\n";
}
