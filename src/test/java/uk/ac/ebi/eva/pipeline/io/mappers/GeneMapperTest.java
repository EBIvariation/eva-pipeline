package uk.ac.ebi.eva.pipeline.io.mappers;

<<<<<<< HEAD
import org.junit.Test;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.test.data.GtfStaticTestData;

import static org.junit.Assert.assertNotNull;
=======
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneLineMapper;
import org.junit.Test;
import uk.ac.ebi.eva.test.data.GtfStaticTestData;

import static junit.framework.TestCase.assertNotNull;
>>>>>>> 15d9dcd2d437c46bd24fc5e16ea5058ff22648b6

public class GeneMapperTest {

    @Test
    public void shouldMapAllFieldsInGtf() throws Exception {
        GeneLineMapper lineMapper = new GeneLineMapper();
        for (String gtfLine : GtfStaticTestData.GTF_CONTENT.split(GtfStaticTestData.GTF_LINE_SPLIT)) {
            if (!gtfLine.startsWith(GtfStaticTestData.GTF_COMMENT_LINE)) {
                FeatureCoordinates gene = lineMapper.mapLine(gtfLine, 0);
                assertNotNull(gene.getChromosome());
            }
        }
    }

}
