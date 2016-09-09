package uk.ac.evi.eva.pipeline.io.processor;

import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneFilterProcessor;
import embl.ebi.variation.eva.pipeline.steps.readers.GeneReader;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import uk.ac.evi.eva.test.data.GtfStaticTestData;

import java.io.File;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.makeGzipFile;
import static junit.framework.TestCase.assertEquals;

public class GeneFilterProcessorTest {

    @Test
    public void shouldKeepGenesAndTranscripts() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        GeneFilterProcessor geneFilterProcessor = new GeneFilterProcessor();

        //simulate VEP output file
        File file = makeGzipFile(GtfStaticTestData.GTF_CONTENT);

        GeneReader geneReader = new GeneReader(file);
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

}
