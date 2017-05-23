package uk.ac.ebi.eva.utils;

import org.junit.Test;
import org.opencb.commons.utils.CryptoUtils;

import uk.ac.ebi.eva.commons.models.data.Variant;

import static org.junit.Assert.assertEquals;

public class MongoDBHelperTest {

    @Test
    public void testBuildStorageIdSnv() {
        Variant variant = new Variant("1", 1000, 1000, "A", "C");
        assertEquals("1_1000_A_C", variant.buildVariantId());
    }

    @Test
    public void testBuildStorageIdIndel() {
        Variant variant = new Variant("1", 1000, 1002, "", "CA");
        assertEquals("1_1000__CA", variant.buildVariantId());
    }

    @Test
    public void testBuildStorageIdStructural() {
        String alt = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        Variant variant = new Variant("1", 1000, 1002, "TAG", alt);
        assertEquals("1_1000_TAG_" + new String(CryptoUtils.encryptSha1(alt)), variant.buildVariantId());
    }
}
