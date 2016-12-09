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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link VariantSourceEntryToDBObjectConverter}
 * <p>
 * Input: {@link VariantSourceEntry}
 * output: DBObject representing the {@link VariantSourceEntry}
 */
public class VariantSourceEntryToDBObjectConverterTest {

    private VariantSourceEntry file;

    private BasicDBObject mongoFile;

    private DBObject mongoFileWithIds;

    @Before
    public void setUp() {
        // Java native class
        file = new VariantSourceEntry("f1", "s1");
        file.addAttribute("QUAL", "0.01");
        file.addAttribute("AN", "2");
        file.addAttribute("MAX.PROC", "2");
        file.setFormat("GT");

        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        int indexNa001 = file.addSampleData(na001);
        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        int indexNa002 = file.addSampleData(na002);
        Map<String, String> na003 = new HashMap<>();
        na003.put("GT", "1/1");
        int indexNa003 = file.addSampleData(na003);

        // MongoDB object
        mongoFile = new BasicDBObject(VariantSourceEntryToDBObjectConverter.FILEID_FIELD, file.getFileId())
                .append(VariantSourceEntryToDBObjectConverter.STUDYID_FIELD, file.getStudyId())
                .append(VariantSourceEntryToDBObjectConverter.FORMAT_FIELD, file.getFormat());

        BasicDBObject attributes = new BasicDBObject("QUAL", "0.01")
                .append("AN", "2")
                .append("MAX" + VariantSourceEntryToDBObjectConverter.CHARACTER_TO_REPLACE_DOTS + "PROC", "2");
        mongoFile.append(VariantSourceEntryToDBObjectConverter.ATTRIBUTES_FIELD, attributes);

        BasicDBObject genotypeCodes = new BasicDBObject();
        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Arrays.asList(1));
        genotypeCodes.append("1/1", Arrays.asList(2));
        mongoFile.append(VariantSourceEntryToDBObjectConverter.SAMPLES_FIELD, genotypeCodes);

        mongoFileWithIds = new BasicDBObject((this.mongoFile.toMap()));
        mongoFileWithIds.put("samp", new BasicDBObject());
        ((DBObject) mongoFileWithIds.get("samp")).put("def", "0/0");
        ((DBObject) mongoFileWithIds.get("samp")).put("0/1", Arrays.asList(indexNa002));
        ((DBObject) mongoFileWithIds.get("samp")).put("1/1", Arrays.asList(indexNa003));
    }

    @Test
    public void testConvertToStorageTypeWithoutSamples() {
        VariantSourceEntryToDBObjectConverter converter;
        converter = new VariantSourceEntryToDBObjectConverter(new SamplesToDBObjectConverter());
        DBObject converted = converter.convert(file);
        assertEquals(mongoFile, converted);
    }

    @Test
    public void testConvertToStorageTypeWithSamples() {
        VariantSourceEntryToDBObjectConverter converter;
        converter = new VariantSourceEntryToDBObjectConverter(new SamplesToDBObjectConverter());
        DBObject convertedMongo = converter.convert(file);
        assertEquals(mongoFileWithIds, convertedMongo);
    }
}
