package uk.ac.ebi.eva.test.data;

import uk.ac.ebi.eva.pipeline.jobs.VariantAnnotConfigurationTest;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class VariantData {

    private static final String VARIANT_WITHOUT_ANNOTATION_PATH = "/annotation/VariantWithOutAnnotation";
    private static final String VARIANT_WITH_ANNOTATION_PATH = "/annotation/VariantWithAnnotation";

    public static String getVariantWithoutAnnotation() throws IOException {
        URL variantWithNoAnnotationUrl = VariantData.class.getResource(VARIANT_WITHOUT_ANNOTATION_PATH);
        return FileUtils.readFileToString(new File(variantWithNoAnnotationUrl.getFile()));
    }


    public static String getVariantWithAnnotation() throws IOException {
        URL variantWithAnnotationUrl = VariantAnnotConfigurationTest.class.getResource(VARIANT_WITH_ANNOTATION_PATH);
        return FileUtils.readFileToString(new File(variantWithAnnotationUrl.getFile()));
    }
}
