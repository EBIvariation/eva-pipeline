package uk.ac.ebi.eva.pipeline.io.processor;

import com.mongodb.DBObject;
import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantAnnotationItemProcessor;
import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantWrapper;
import org.junit.Test;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.CommonUtils;

import static junit.framework.TestCase.assertEquals;

public class VariantAnnotationItemProcessorTest {

    @Test
    public void shouldConvertAllFieldsInVariant() throws Exception {
        DBObject dbo = CommonUtils.constructDbo(VariantData.getVariantWithoutAnnotation());

        ItemProcessor<DBObject, VariantWrapper> processor =  new VariantAnnotationItemProcessor();
        VariantWrapper variant = processor.process(dbo);
        assertEquals("+", variant.getStrand());
        assertEquals("20", variant.getChr());
        assertEquals("G/A", variant.getRefAlt());
        assertEquals(60343, variant.getEnd());
        assertEquals(60343, variant.getStart());
    }

}
