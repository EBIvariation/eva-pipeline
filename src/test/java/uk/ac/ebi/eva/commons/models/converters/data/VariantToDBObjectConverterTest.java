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

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;
import uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.HgvsMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.VariantAtMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.test.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.test.configuration.MongoOperationConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests the automatic conversion of {@link VariantMongo} to {@link DBObject}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoOperationConfiguration.class, MongoCollectionNameConfiguration.class})
public class VariantToDBObjectConverterTest {

    @Autowired
    private MongoOperations mongoOperations;

    private VariantMongo buildVariantDocument(VariantSourceEntryMongo variantSource, boolean withIds) {
        Set<VariantSourceEntryMongo> variantSources = variantSource == null ? null :
                Collections.singleton(variantSource);
        VariantMongo variant = new VariantMongo(
                VariantMongo.buildVariantId("1", 1000, "A", "C"),
                VariantType.SNV,
                "1",
                1000,
                1000,
                1,
                "A",
                "C",
                new VariantAtMongo("1_1_1k", "1_0_10k"),
                Collections.singleton(new HgvsMongo("genomic", "1:g.1000A>C")),
                withIds == true ? Collections.singleton("rs666") : null,
                withIds == true ? "rs666" : null,
                variantSources,
                null,
                null
        );

        return variant;
    }

    private VariantSourceEntryMongo buildVariantSourceEntryWithSamples() {
        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        na001.put("DP", "4");

        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        na002.put("DP", "5");

        List<Map<String, String>> samples = new ArrayList<>();
        samples.add(na001);
        samples.add(na002);

        return new VariantSourceEntryMongo(
                "f1",
                "s1",
                null,
                buildAttributes(),
                "GT:DP",
                samples
        );
    }

    private VariantSourceEntryMongo buildVariantSourceEntryWithoutSamples() {
        return new VariantSourceEntryMongo(
                "f1",
                "s1",
                null,
                buildAttributes()
        );
    }

    private Map buildAttributes() {
        HashMap attributes = new HashMap();
        attributes.put("QUAL", "0.01");
        attributes.put("AN", "2");
        return attributes;
    }

    private Document buildMongoVariant(boolean withFiles) {
        //Setup variant
        Variant variant = new Variant("1", 1000, 1000, "A", "C");
        variant.setIds(Collections.singleton("rs666"));
        variant.setMainId("rs666");

        //Setup variantSourceEntry
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry("f1", "s1");
        variantSourceEntry.addAttribute("QUAL", "0.01");
        variantSourceEntry.addAttribute("AN", "2");
        variantSourceEntry.setFormat("GT:DP");

        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        na001.put("DP", "4");
        variantSourceEntry.addSampleData(na001);
        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        na002.put("DP", "5");
        variantSourceEntry.addSampleData(na002);
        variant.addSourceEntry(variantSourceEntry);

        HashMap attributes = new HashMap();
        attributes.put("QUAL", "0.01");
        attributes.put("AN", "2");

        List<Map<String, String>> samples = new ArrayList<>();
        samples.add(na001);
        samples.add(na002);

        //Setup mongoVariant
        Document mongoVariant = new Document("_id", "1_1000_A_C")
                .append(VariantMongo.TYPE_FIELD, variant.getType().name())
                .append(VariantMongo.CHROMOSOME_FIELD, variant.getChromosome())
                .append(VariantMongo.START_FIELD, variant.getStart())
                .append(VariantMongo.END_FIELD, variant.getStart())
                .append(VariantMongo.LENGTH_FIELD, variant.getLength())
                .append(VariantMongo.REFERENCE_FIELD, variant.getReference())
                .append(VariantMongo.ALTERNATE_FIELD, variant.getAlternate());

        List<String> chunkIds = new ArrayList<>();
        chunkIds.add("1_0_10k");
        chunkIds.add("1_1_1k");
        mongoVariant.append("_at", new Document("chunkIds", chunkIds));

        List<Document> hgvs = new ArrayList<>();
        hgvs.add(new Document("type", "genomic").append("name", "1:g.1000A>C"));
        mongoVariant.append("hgvs", hgvs);
        mongoVariant.append(VariantMongo.IDS_FIELD, new ArrayList<>(variant.getIds()));
        mongoVariant.append(VariantMongo.MAIN_ID_FIELD, variant.getMainId());

        if (withFiles) {
            // MongoDB object
            Document mongoFile = new Document(VariantSourceEntryMongo.FILEID_FIELD,
                    variantSourceEntry.getFileId())
                    .append(VariantSourceEntryMongo.STUDYID_FIELD, variantSourceEntry.getStudyId())
                    .append(VariantSourceEntryMongo.ATTRIBUTES_FIELD,
                            new Document("QUAL", "0.01").append("AN", "2"))
                    .append(VariantSourceEntryMongo.FORMAT_FIELD, variantSourceEntry.getFormat());

            Document genotypeCodes = new Document();
            genotypeCodes.append("def", "0/0");
            genotypeCodes.append("0/1", Collections.singletonList(1));
            mongoFile.append(VariantSourceEntryMongo.SAMPLES_FIELD, genotypeCodes);
            BasicDBList files = new BasicDBList();
            files.add(mongoFile);
            mongoVariant.append("files", files);
        } else {
            mongoVariant.append("files", new ArrayList<>());
        }

        mongoVariant.append("st", new ArrayList<>());
        mongoVariant.append("annot", new ArrayList<>());

        return mongoVariant;
    }

    @Test
    public void testConvertToStorageTypeWithFiles() {
        Document converted = (Document) mongoOperations.getConverter().convertToMongoType(
                buildVariantDocument(buildVariantSourceEntryWithSamples(), true)
        );

        assertEquals(buildMongoVariant(true), converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutFiles() {
        Document converted = (Document) mongoOperations.getConverter().convertToMongoType(
                buildVariantDocument(null, true)
        );

        assertEquals(buildMongoVariant(false), converted);
    }

    @Test
    public void testConvertToStorageTypeNullIds() {
        Document converted = (Document) mongoOperations.getConverter().convertToMongoType(
                buildVariantDocument(null, false)
        );

        Document mongoVariant = buildMongoVariant(false);
        mongoVariant.remove(VariantMongo.MAIN_ID_FIELD);
        mongoVariant.append(VariantMongo.IDS_FIELD, new ArrayList<>());
        assertEquals(mongoVariant, converted);
    }

    @Test
    public void testConvertToStorageTypeEmptyIds() {
        Document converted = (Document) mongoOperations.getConverter().convertToMongoType(
                buildVariantDocument(null, false)
        );

        Document mongoVariant = buildMongoVariant(false);
        mongoVariant.remove(VariantMongo.MAIN_ID_FIELD);
        mongoVariant.append(VariantMongo.IDS_FIELD, new ArrayList<>());
        assertEquals(mongoVariant, converted);
    }

}
