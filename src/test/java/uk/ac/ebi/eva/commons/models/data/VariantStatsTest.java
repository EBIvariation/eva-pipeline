package uk.ac.ebi.eva.commons.models.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VariantStatsTest {
    @Test
    public void testChangeRefAltToUpperCase() {
        VariantStats variantStats = new VariantStats("a", "t", null);
        assertEquals("A", variantStats.getRefAllele());
        assertEquals("T", variantStats.getAltAllele());
    }
}
