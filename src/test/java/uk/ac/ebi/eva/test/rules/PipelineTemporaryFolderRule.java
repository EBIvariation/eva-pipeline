package uk.ac.ebi.eva.test.rules;

import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

public class PipelineTemporaryFolderRule extends TemporaryFolder {

    private Logger logger = LoggerFactory.getLogger(PipelineTemporaryFolderRule.class);

    private Description description;

    @Override
    protected void before() throws Throwable {
        super.before();
        String className = description.getClassName() == null ? "" : description.getClassName();
        String methodName = description.getMethodName() == null ? "" : description.getMethodName();
        logger.debug("temporary folder for Class '" + className + "', method '" + methodName + "' path '" +
                getRoot().getAbsolutePath() + "'");
    }

    @Override
    public Statement apply(Statement base, Description description) {
        this.description = description;
        return super.apply(base, description);
    }

    /**
     * Creates a temporary GzipFile withe the content at {@param content}. This file is marked to be deleted by java
     * after finishing the test process.
     * @param content
     * @return
     * @throws IOException
     */
    public File newGzipFile(String content) throws IOException {
        File tempFile = newFile();
        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(content);
            }
        }
        return tempFile;
    }

    /**
     * Creates a temporary GzipFile withe the content at {@param content}. This file is marked to be deleted by java
     * after finishing the test process.
     * @param content
     * @param name how the temporal file will be called under the temporal folder
     * @return
     * @throws IOException
     */
    public File newGzipFile(String content, String name) throws IOException {
        File tempFile = newFile(name);
        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(content);
            }
        }
        return tempFile;
    }
}
