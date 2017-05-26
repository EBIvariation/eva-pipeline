/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

/**
 * Utility methods to work with compressed files.
 */
public class CompressionHelper {

    public static boolean isGzip(String file) throws IOException {
        return isGzip(new File(file));
    }

    public static boolean isGzip(File file) throws IOException {
        try {
            GZIPInputStream inputStream = new GZIPInputStream(new FileInputStream(file));
            inputStream.close();
        } catch (ZipException exception) {
            return false;
        }
        return true;
    }

    public static byte[] gzip(String text) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BufferedOutputStream bufos = new BufferedOutputStream(new GZIPOutputStream(bos));

        try {
            bufos.write(text.getBytes());
        } finally {
            bufos.close();
        }

        byte[] retval = bos.toByteArray();
        bos.close();
        return retval;
    }
}
