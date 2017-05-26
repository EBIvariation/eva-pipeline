package uk.ac.ebi.eva.utils;

import org.junit.Test;
import org.opencb.commons.utils.CryptoUtils;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;

import static org.junit.Assert.assertEquals;

public class MongoDBHelperTest {

    @Test
    public void testBuildStorageIdSnv() {
        assertEquals("1_1000_A_C", VariantDocument.buildVariantId(
                "1",
                1000,
                "A",
                "C"
        ));
    }

    @Test
    public void testBuildStorageIdIndel() {
        assertEquals("1_1000__CA", VariantDocument.buildVariantId(
                "1",
                1000,
                "",
                "CA"
        ));
    }

    @Test
    public void testBuildStorageIdStructural() {
        String alt = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        assertEquals("1_1000_TAG_" + new String(CryptoUtils.encryptSha1(alt)), VariantDocument.buildVariantId(
                "1",
                1000,
                "TAG",
                alt
        ));
    }
}
