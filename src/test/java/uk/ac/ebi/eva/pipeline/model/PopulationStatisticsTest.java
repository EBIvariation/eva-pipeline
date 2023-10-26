package uk.ac.ebi.eva.pipeline.model;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class PopulationStatisticsTest {
    @Test
    public void testChangeRefAltToUpperCase() {
        PopulationStatistics populationStatistics = new PopulationStatistics("variantId", "chr", 1,
                "a", "t", "cohortId", "studyId", -1, -1, null, null,
                -1, -1, new HashMap<>());
        assertEquals("A", populationStatistics.getReference());
        assertEquals("T", populationStatistics.getAlternate());
    }
}
