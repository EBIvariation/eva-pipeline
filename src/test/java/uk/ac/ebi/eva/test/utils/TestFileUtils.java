package uk.ac.ebi.eva.test.utils;

import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

public abstract class TestFileUtils {

    /** use {@link uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule#newGzipFile(java.lang.String)} instead */
    @Deprecated
    public static File makeGzipFile(String content, String vepOutput) throws IOException {
        File tempFile = new File(vepOutput);
        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(content);
            }
        }
        return tempFile;
    }

    public static void copyResource(String resourcePath, String outputDir) throws IOException {
        File vcfFile = new File(TestFileUtils.class.getResource(resourcePath).getFile());
        FileCopyUtils.copy(vcfFile, new File(outputDir, vcfFile.getName()));
    }

    public static URL getResourceUrl(String resourcePath) {
        return TestFileUtils.class.getResource(resourcePath);
    }

}
