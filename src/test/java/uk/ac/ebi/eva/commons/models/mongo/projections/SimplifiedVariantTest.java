package uk.ac.ebi.eva.commons.models.mongo.projections;

import org.junit.Test;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.projections.SimplifiedVariant;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class SimplifiedVariantTest {

    @Test
    public void testChangeRefAltToUpperCase() {
        SimplifiedVariant simplifiedVariant = new SimplifiedVariant(Variant.VariantType.SNV, "chr",
                1, 2, 1, "a", "t", new HashMap<>());
        assertEquals("A", simplifiedVariant.getReference());
        assertEquals("T", simplifiedVariant.getAlternate());
    }

}
