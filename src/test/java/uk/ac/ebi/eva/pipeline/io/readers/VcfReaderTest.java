package uk.ac.ebi.eva.pipeline.io.readers;

import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;

/**
 * {@link VcfReader}
 * input: a Vcf file
 * output: a VariantAnnotation each time its `.read()` is called
 *
 * incorrect input lines should not make the reader fail.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VcfReaderTest {

    @Test
    public void shouldReadAllLines() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        final String inputFilePath = "/small20.vcf.gz";
        String inputFile = VcfReaderTest.class.getResource(inputFilePath).getFile();

        String fileId = "5";
        String studyId = "7";
        String studyName = "study name";
        VariantSource source = new VariantSource(inputFile,
                fileId,
                studyId,
                studyName,
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        VcfReader vcfReader = new VcfReader(source, inputFile);

        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        List<Variant> variants;

        int count = 0;

        while ((variants = vcfReader.read()) != null) {
            count++;
        }

        // VcfReader should get all the lines from the file
        long actualCount = JobTestUtils.getLines(new GZIPInputStream(new FileInputStream(inputFile)));
        assertEquals(actualCount, count);
    }
}
