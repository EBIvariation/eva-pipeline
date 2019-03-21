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
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.test.configuration.MongoOperationConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo.ATTRIBUTES_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo.CHARACTER_TO_REPLACE_DOTS;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo.FILEID_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo.STUDYID_FIELD;

/**
 * Tests automatic conversion from {@link VariantSourceEntryMongo} to {@link DBObject}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoOperationConfiguration.class})
public class VariantSourceEntryToDBObjectConverterTest {

    @Autowired
    private MongoOperations mongoOperations;

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
        mongoFile = new BasicDBObject(FILEID_FIELD, file.getFileId())
                .append(STUDYID_FIELD, file.getStudyId());

        BasicDBObject attributes = new BasicDBObject("QUAL", "0.01")
                .append("AN", "2")
                .append("MAX" + CHARACTER_TO_REPLACE_DOTS + "PROC", "2");
        mongoFile.append(ATTRIBUTES_FIELD, attributes);

        mongoFileWithIds = new BasicDBObject((this.mongoFile.toMap()));
        mongoFileWithIds.put("samp", new BasicDBObject());
        ((DBObject) mongoFileWithIds.get("samp")).put("def", "0/0");
        ((DBObject) mongoFileWithIds.get("samp")).put("0/1", Arrays.asList(indexNa002));
        ((DBObject) mongoFileWithIds.get("samp")).put("1/1", Arrays.asList(indexNa003));
    }

    @Test
    public void testConvertToStorageTypeWithoutSamples() {
        VariantSourceEntryMongo variantSource = new VariantSourceEntryMongo(
                file.getFileId(),
                file.getStudyId(),
                file.getSecondaryAlternates(),
                file.getAttributes()
        );
        assertEquals(mongoFile, mongoOperations.getConverter().convertToMongoType(variantSource));
    }

    @Test
    public void testConvertToStorageTypeWithSamples() {
        VariantSourceEntryMongo variantSource = new VariantSourceEntryMongo(
                file.getFileId(),
                file.getStudyId(),
                file.getSecondaryAlternates(),
                file.getAttributes(),
                file.getFormat(),
                file.getSamplesData()
        );
        assertEquals(mongoFileWithIds, mongoOperations.getConverter().convertToMongoType(variantSource));
    }
}
