package uk.ac.ebi.eva.commons.models.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VariantTest {
    @Test
    public void testChangeRefAltToUpperCase() {
        Variant variant = new Variant("1", 1, 1, "c", "t");
        assertEquals("C", variant.getReference());
        assertEquals("T", variant.getAlternate());
    }
}
