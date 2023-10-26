package uk.ac.ebi.eva.commons.models.mongo;

import org.junit.Test;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class VariantDocumentTest {
    @Test
    public void testChangeRefAltToUpperCase() {
        VariantDocument variantDocument = new VariantDocument(Variant.VariantType.SNV, "chr", 1,
                2, 1, "a", "t", (Map<String, Set<String>>) null,
                null, null);

        assertEquals("A", variantDocument.getReference());
        assertEquals("T", variantDocument.getAlternate());
    }
}
