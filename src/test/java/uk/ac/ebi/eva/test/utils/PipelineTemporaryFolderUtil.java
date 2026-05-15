package uk.ac.ebi.eva.test.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.zip.GZIPOutputStream;

public class PipelineTemporaryFolderUtil {

    private static final Logger logger = LoggerFactory.getLogger(PipelineTemporaryFolderUtil.class);

    private final Path root;

    public PipelineTemporaryFolderUtil() {
        try {
            this.root = Files.createTempDirectory("pipeline-test-");
            this.root.toFile().deleteOnExit();
            logger.debug("temporary folder path '{}'", root.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create temporary folder", e);
        }
    }

    public File getRoot() {
        return root.toFile();
    }

    public File newFolder() throws IOException {
        File folder = Files.createTempDirectory(root, "tmp-dir-").toFile();
        folder.deleteOnExit();
        return folder;
    }

    public File newFolder(String name) throws IOException {
        File folder = root.resolve(name).toFile();
        if (!folder.exists() && !folder.mkdirs()) {
            throw new IOException("Could not create directory: " + folder);
        }
        folder.deleteOnExit();
        return folder;
    }

    public File newFile() throws IOException {
        File tempFile = Files.createTempFile(root, "tmp-", null).toFile();
        tempFile.deleteOnExit();
        return tempFile;
    }

    public File newFile(String name) throws IOException {
        File tempFile = root.resolve(name).toFile();
        tempFile.getParentFile().mkdirs();
        if (!tempFile.exists() && !tempFile.createNewFile()) {
            throw new IOException("Could not create file: " + tempFile);
        }
        tempFile.deleteOnExit();
        return tempFile;
    }

    public File newGzipFile(String content) throws IOException {
        return newGzipFile(content, null);
    }

    public File newGzipFile(String content, String name) throws IOException {
        File tempFile = (name == null || name.isBlank())
                ? Files.createTempFile(root, "tmp-", ".gz").toFile()
                : root.resolve(name).toFile();

        tempFile.deleteOnExit();

        try (FileOutputStream output = new FileOutputStream(tempFile);
             Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), StandardCharsets.UTF_8)) {
            writer.write(content);
        }

        return tempFile;
    }

    public void clear() {
        deleteRecursively();
    }

    public void deleteRecursively() {
        try {
            Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete temporary folder", e);
        }
    }
}