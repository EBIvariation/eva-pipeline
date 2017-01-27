package uk.ac.ebi.eva.test.utils;

import org.springframework.util.FileCopyUtils;

import java.io.*;
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
        FileCopyUtils.copy(vcfFile, new File(outputDir, resourcePath));
    }

    public static File getResource(String resourcePath) {
        return new File(TestFileUtils.class.getResource(resourcePath).getFile());
    }

    public static URL getResourceUrl(String resourcePath) {
        return TestFileUtils.class.getResource(resourcePath);
    }

}
