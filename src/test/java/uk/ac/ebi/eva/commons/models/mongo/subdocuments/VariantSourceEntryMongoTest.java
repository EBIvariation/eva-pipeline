package uk.ac.ebi.eva.commons.models.mongo.subdocuments;

import org.junit.Test;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;

import java.lang.reflect.Field;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class VariantSourceEntryMongoTest {
    @Test
    public void testChangeRefAltToUpperCase() throws NoSuchFieldException, IllegalAccessException {
        VariantSourceEntryMongo variantSourceEntryMongo = new VariantSourceEntryMongo(null, null,
                new String[]{"a", "t"}, new HashMap<>());

        Field altField = VariantSourceEntryMongo.class.getDeclaredField("alternates");
        altField.setAccessible(true);
        String[] altValue = (String[]) altField.get(variantSourceEntryMongo);

        assertEquals("A", altValue[0]);
        assertEquals("T", altValue[1]);
    }
}
