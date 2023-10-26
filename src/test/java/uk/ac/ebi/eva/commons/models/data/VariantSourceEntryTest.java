package uk.ac.ebi.eva.commons.models.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VariantSourceEntryTest {
    @Test
    public void testChangeRefAltToUpperCase() {
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry(null, null, new String[]{"a", "t"}, null);
        assertEquals("A", variantSourceEntry.getSecondaryAlternates()[0]);
        assertEquals("T", variantSourceEntry.getSecondaryAlternates()[1]);
    }
}
