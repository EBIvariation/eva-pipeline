package uk.ac.ebi.eva.pipeline.io.writers;

import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantWrapper;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.CommonUtils;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import embl.ebi.variation.eva.pipeline.steps.writers.VepInputWriter;
import org.junit.Test;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class VepInputWriterTest {

    @Test
    public void vepInputWriterShouldWriteAllFieldsToFile() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        DBObjectToVariantConverter converter = new DBObjectToVariantConverter();
        VariantWrapper variant = new VariantWrapper(converter.convertToDataModelType(CommonUtils.constructDbo(VariantData.getVariantWithAnnotation())));

        File tempFile = JobTestUtils.createTempFile();
        VepInputWriter writer = new VepInputWriter(tempFile);
        writer.open(executionContext);
        writer.write(Collections.singletonList(variant));
        assertEquals("20\t60344\t60348\tG/A\t+", CommonUtils.readFirstLine(tempFile));
        writer.close();
    }

}
