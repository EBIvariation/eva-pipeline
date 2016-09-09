package uk.ac.evi.eva.pipeline.io.mappers;

import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneLineMapper;
import org.junit.Test;
import uk.ac.evi.eva.test.data.GtfStaticTestData;

import static junit.framework.TestCase.assertNotNull;

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
