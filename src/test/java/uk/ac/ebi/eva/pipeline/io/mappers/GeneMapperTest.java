package uk.ac.ebi.eva.pipeline.io.mappers;

import org.junit.Test;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.test.data.GtfStaticTestData;

import static org.junit.Assert.assertNotNull;

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
