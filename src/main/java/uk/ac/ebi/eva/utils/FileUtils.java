/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.io.GzipLazyResource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

public abstract class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static void validateDirectoryPath(String path, boolean emptyIsValid) throws FileNotFoundException {
        if (emptyIsValid && (path == null || path.isEmpty())) {
            return;
        }
        File file = new File(path);
        if (!file.exists()) {
            throw new FileNotFoundException("Path '" + path + "' doesn't exist.");
        }
        if (!file.isDirectory()) {
            throw new FileNotFoundException("Path '" + path + "' is not a directory.");
        }
    }

    public static URI getPathUri(String path, boolean emptyIsValid) throws FileNotFoundException, URISyntaxException {
        validateDirectoryPath(path, emptyIsValid);
        return URLHelper.createUri(path);
    }

    public static Resource getResource(File file) throws IOException {
        Resource resource;
        if (CompressionHelper.isGzip(file)) {
            resource = new GzipLazyResource(file);
        } else {
            resource = new FileSystemResource(file);
        }
        return resource;
    }

    public static File getResource(String resourcePath) {
        return new File(FileUtils.class.getResource(resourcePath).getFile());
    }

    public static InputStream getResourceAsStream(String resourcePath) {
        return FileUtils.class.getResourceAsStream(resourcePath);
    }

    public static Properties getPropertiesFile(InputStream propertiesInputStream) throws IOException {
        Properties properties = new Properties();
        properties.load(propertiesInputStream);
        return properties;
    }

    /**
     * Creates a temporary GzipFile withe the content at {@param content}.
     * @param content
     * @param name how the temporal file will be called
     * @return
     * @throws IOException
     */
    public static File newGzipFile(String content, String name) throws IOException {
        File tempFile = File.createTempFile(name, ".gz");
        try (FileOutputStream output = new FileOutputStream(tempFile)) {
            try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8")) {
                writer.write(content);
            }
        }
        return tempFile;
    }
}
