package uk.ac.ebi.eva.pipeline.io.mappers;

import org.junit.Test;
import uk.ac.ebi.eva.commons.core.models.IVariant;

import static org.junit.Assert.assertEquals;

public class AccessionReportLineMapperTest {

    @Test
    public void testChangeRefAltToUpperCase() {
        IVariant variant = new AccessionReportLineMapper().mapLine("chr\t1\tid\ta\tt\tabc", 1);

        assertEquals("A", variant.getReference());
        assertEquals("T", variant.getAlternate());
    }
}
