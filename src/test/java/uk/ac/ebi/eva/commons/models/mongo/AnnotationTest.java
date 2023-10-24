package uk.ac.ebi.eva.commons.models.mongo;

import org.junit.Test;
import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;

import static org.junit.Assert.assertEquals;

public class AnnotationTest {

    @Test
    public void testChangeRefAltToUpperCase() {
        Annotation annotation = new Annotation("chr", 1, 1, "a", "t",
                "vep", "vep_cache");
        assertEquals("chr_1_A_T_vep_vep_cache", annotation.getId());
    }
}
