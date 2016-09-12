package uk.ac.ebi.eva.pipeline.io.mappers;

<<<<<<< HEAD
=======
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationLineMapper;
>>>>>>> 15d9dcd2d437c46bd24fc5e16ea5058ff22648b6
import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import uk.ac.ebi.eva.test.data.VepOutputContent;

import static junit.framework.TestCase.assertNotNull;

public class VariantAnnotationMapperTest {

    @Test
    public void shouldReadAllFieldsInVepOutput() throws Exception {
        VariantAnnotationLineMapper lineMapper = new VariantAnnotationLineMapper();
        for (String annotLine : VepOutputContent.vepOutputContent.split("\n")) {
            VariantAnnotation variantAnnotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(variantAnnotation.getConsequenceTypes());
        }
    }

}
