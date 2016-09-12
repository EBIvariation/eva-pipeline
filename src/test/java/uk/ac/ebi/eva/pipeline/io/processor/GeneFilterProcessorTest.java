package uk.ac.ebi.eva.pipeline.io.processor;

<<<<<<< HEAD
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import uk.ac.ebi.eva.pipeline.io.readers.GeneReader;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.GeneFilterProcessor;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
=======
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneFilterProcessor;
import embl.ebi.variation.eva.pipeline.steps.readers.GeneReader;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
>>>>>>> 15d9dcd2d437c46bd24fc5e16ea5058ff22648b6
import uk.ac.ebi.eva.test.data.GtfStaticTestData;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;

<<<<<<< HEAD
=======
import static uk.ac.ebi.eva.test.utils.JobTestUtils.makeGzipFile;
>>>>>>> 15d9dcd2d437c46bd24fc5e16ea5058ff22648b6
import static junit.framework.TestCase.assertEquals;

public class GeneFilterProcessorTest {

    @Test
    public void shouldKeepGenesAndTranscripts() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        GeneFilterProcessor geneFilterProcessor = new GeneFilterProcessor();

        //simulate VEP output file
        File file = JobTestUtils.makeGzipFile(GtfStaticTestData.GTF_CONTENT);

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
