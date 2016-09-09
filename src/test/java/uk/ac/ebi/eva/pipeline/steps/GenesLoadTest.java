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
package uk.ac.ebi.eva.pipeline.steps;

import com.mongodb.*;

import uk.ac.ebi.eva.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.config.InitDBConfig;
import uk.ac.ebi.eva.pipeline.gene.FeatureCoordinates;
import uk.ac.ebi.eva.pipeline.gene.GeneFilterProcessor;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.io.readers.GeneReader;
import uk.ac.ebi.eva.pipeline.io.writers.GeneWriter;
import uk.ac.ebi.eva.pipeline.jobs.JobTestUtils;
import uk.ac.ebi.eva.pipeline.steps.GenesLoad;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.*;
import static uk.ac.ebi.eva.pipeline.jobs.JobTestUtils.makeGzipFile;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test {@link GenesLoad}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { GenesLoad.class, InitDBConfig.class,})
public class GenesLoadTest {

    private ItemProcessor<FeatureCoordinates, FeatureCoordinates> geneFilterProcessor;

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    private ExecutionContext executionContext;

    @Before
    public void setUp() throws Exception {
        geneFilterProcessor =  new GeneFilterProcessor();
        variantJobsArgs.loadArgs();
        executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
    }

    @Test
    public void geneReaderShouldReadAllFieldsInGtf() throws Exception {
        GeneLineMapper lineMapper = new GeneLineMapper();
        for (String gtfLine : gtfContent.split("\n")) {
            if (!gtfLine.startsWith("#")) {
                FeatureCoordinates gene = lineMapper.mapLine(gtfLine, 0);
                assertNotNull(gene.getChromosome());
            }
        }
    }

    @Test
    public void geneReaderShouldReadAllLinesInGtf() throws Exception {
        String gtf = variantJobsArgs.getPipelineOptions().getString("input.gtf");

        //simulate VEP output file
        makeGzipFile(gtfContent, gtf);

        GeneReader geneReader = new GeneReader(variantJobsArgs.getPipelineOptions());
        geneReader.setSaveState(false);
        geneReader.open(executionContext);

        FeatureCoordinates gene;
        int chromosomeCount = 0;
        int count = 0;
        while ((gene = geneReader.read()) != null) {
            count++;
            if (gene.getChromosome() != null && !gene.getChromosome().isEmpty()) {
                chromosomeCount++;
            }
        }
        // all should have at least consequence type annotations
        assertEquals(count, chromosomeCount);

        // variantAnnotationReader should get all the lines from the file
        long actualCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(gtf)));
        assertEquals(actualCount, count);
    }

    @Test
    public void geneFilterProcessorShouldKeepGenesAndTranscripts() throws Exception {
        String gtf = variantJobsArgs.getPipelineOptions().getString("input.gtf");

        //simulate VEP output file
        makeGzipFile(gtfContent, gtf);

        GeneReader geneReader = new GeneReader(variantJobsArgs.getPipelineOptions());
        geneReader.setSaveState(false);
        geneReader.open(executionContext);

        FeatureCoordinates gene;
        int count = 0;
        int keptGenes = 0;
        while ((gene = geneReader.read()) != null) {
            count++;
            FeatureCoordinates processedGene = geneFilterProcessor.process(gene);
            if (processedGene != null) {
                keptGenes++;
            }
        }

        assertEquals(7, count);
        assertEquals(4, keptGenes);
    }

    @Test
    public void geneWriterShouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        String dbCollectionGenesName = variantJobsArgs.getPipelineOptions().getString("db.collections.features.name");
        JobTestUtils.cleanDBs(dbName);

        GeneWriter geneWriter = new GeneWriter(variantJobsArgs.getPipelineOptions());

        GeneLineMapper lineMapper = new GeneLineMapper();
        List<FeatureCoordinates> genes = new ArrayList<>();
        for (String gtfLine : gtfContent.split("\n")) {
            if (!gtfLine.startsWith("#")) {
                genes.add(lineMapper.mapLine(gtfLine, 0));
            }
        }
        geneWriter.write(genes);

        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection =
                mongoClient.getDB(dbName).getCollection(dbCollectionGenesName);

        // count documents in DB and check they have region (chr + start + end)
        DBCursor cursor = genesCollection.find();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject next = cursor.next();
            assertTrue(next.get("chromosome") != null);
            assertTrue(next.get("start") != null);
            assertTrue(next.get("end") != null);
        }
        assertEquals(genes.size(), count);

//        JobTestUtils.cleanDBs(dbName);
    }

    private final String gtfContent = "" +
            "#!genome-build ChlSab1.1\n" +
            "#!genome-version ChlSab1.1\n" +
            "#!genome-date 2014-03\n" +
            "#!genome-build-accession NCBI:GCA_000409795.2\n" +
            "#!genebuild-last-updated 2015-02\n" +
            "8\tensembl\tgene\t183180\t246703\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\";\n" +
            "8\tensembl\ttranscript\t183180\t246703\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; transcript_id \"ENSCSAT00000015163\"; transcript_version \"1\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\"; transcript_name \"FBXO25-201\"; transcript_source \"ensembl\"; transcript_biotype \"protein_coding\";\n" +
            "8\tensembl\texon\t183180\t183285\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; transcript_id \"ENSCSAT00000015163\"; transcript_version \"1\"; exon_number \"1\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\"; transcript_name \"FBXO25-201\"; transcript_source \"ensembl\"; transcript_biotype \"protein_coding\"; exon_id \"ENSCSAE00000108645\"; exon_version \"1\";\n" +
            "8\tensembl\texon\t184547\t184671\t.\t+\t.\tgene_id \"ENSCSAG00000017073\"; gene_version \"1\"; transcript_id \"ENSCSAT00000015163\"; transcript_version \"1\"; exon_number \"2\"; gene_name \"FBXO25\"; gene_source \"ensembl\"; gene_biotype \"protein_coding\"; transcript_name \"FBXO25-201\"; transcript_source \"ensembl\"; transcript_biotype \"protein_coding\"; exon_id \"ENSCSAE00000108644\"; exon_version \"1\";\n" +
            "8\tensembl\tgene\t334894\t335006\t.\t+\t.\tgene_id \"ENSCSAG00000023576\"; gene_version \"1\"; gene_source \"ensembl\"; gene_biotype \"miRNA\";\n" +
            "8\tensembl\ttranscript\t334894\t335006\t.\t+\t.\tgene_id \"ENSCSAG00000023576\"; gene_version \"1\"; transcript_id \"ENSCSAT00000023666\"; transcript_version \"1\"; gene_source \"ensembl\"; gene_biotype \"miRNA\"; transcript_source \"ensembl\"; transcript_biotype \"miRNA\";\n" +
            "8\tensembl\texon\t334894\t335006\t.\t+\t.\tgene_id \"ENSCSAG00000023576\"; gene_version \"1\"; transcript_id \"ENSCSAT00000023666\"; transcript_version \"1\"; exon_number \"1\"; gene_source \"ensembl\"; gene_biotype \"miRNA\"; transcript_source \"ensembl\"; transcript_biotype \"miRNA\"; exon_id \"ENSCSAE00000192318\"; exon_version \"1\";\n";

}
