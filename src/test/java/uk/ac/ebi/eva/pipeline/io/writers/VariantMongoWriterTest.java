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
package uk.ac.ebi.eva.pipeline.io.writers;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Testing {@link VariantMongoWriter}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoConnection.class, MongoMappingContext.class})
public class VariantMongoWriterTest {

    private static final List<? extends Variant> EMPTY_LIST = new ArrayList<>();

    private final String collectionName = "variants";

    @Autowired
    private MongoConnection mongoConnection;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Test
    public void noVariantsNothingShouldBeWritten() throws UnknownHostException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                                                                                mongoMappingContext);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, false);
        variantMongoWriter.doWrite(EMPTY_LIST);

        assertEquals(0, dbCollection.count());
    }

    @Test
    public void variantsShouldBeWrittenIntoMongoDb() throws Exception {
        Variant variant1 = new Variant("1", 1, 2, "A", "T");
        Variant variant2 = new Variant("2", 3, 4, "C", "G");

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                                                                                mongoMappingContext);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        BasicDBObject dbObject = new BasicDBObject();

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, false);
        variantMongoWriter.write(Collections.singletonList(variant1));
        variantMongoWriter.write(Collections.singletonList(variant2));

        assertEquals(2, dbCollection.count());
    }

    @Test
    public void indexesShouldBeCreatedInBackground() throws UnknownHostException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                                                                                mongoMappingContext);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, false);

        List<DBObject> indexInfo = dbCollection.getIndexInfo();

        Set<String> createdIndexes = indexInfo.stream().map(index -> index.get("name").toString())
                .collect(Collectors.toSet());
        Set<String> expectedIndexes = new HashSet<>();
        expectedIndexes.addAll(Arrays.asList("so_1", "chr_1_start_1_end_1", "files.sid_1_files.fid_1", "_id_",
                "ids_1"));
        assertEquals(expectedIndexes, createdIndexes);

        indexInfo.stream().filter(index -> !("_id_".equals(index.get("name").toString())))
                .forEach(index -> assertEquals("true", index.get(MongoDBHelper.BACKGROUND_INDEX).toString()));

    }

    @Test
    public void writeTwiceSameVariantShouldUpdate() throws Exception {
        Variant variant1 = new Variant("1", 1, 2, "A", "T");
        variant1.addSourceEntry(new VariantSourceEntry("test_file", "test_study_id"));

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                                                                                mongoMappingContext);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, false);
        variantMongoWriter.write(Collections.singletonList(variant1));

        variantMongoWriter.write(Collections.singletonList(variant1));
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);
        assertEquals(1, dbCollection.count());
        final DBObject storedVariant = dbCollection.findOne();
        assertEquals(1, ((BasicDBList) storedVariant.get("files")).size());
    }

    @Test
    public void allFieldsOfVariantShouldBeStored() throws Exception {
        final String chromosome = "12";
        final int start = 3;
        final int end = 4;
        final String reference = "A";
        final String alternate = "T";
        final String fileId = "fileId";
        final String studyId = "studyId";
        Variant variant = buildVariant(chromosome, start, end, reference, alternate, fileId, studyId);

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                mongoMappingContext);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, true);
        variantMongoWriter.write(Collections.singletonList(variant));

        DBCollection dbCollection = mongoOperations.getCollection(collectionName);
        assertEquals(1, dbCollection.count());
        final DBObject storedVariant = dbCollection.findOne();
        final BasicDBList variantSources = (BasicDBList) storedVariant.get("files");
        assertNotNull(variantSources);
        assertFalse(variantSources.isEmpty());
        assertEquals(fileId, ((DBObject) variantSources.get(0)).get("fid"));
        assertEquals(studyId, ((DBObject) variantSources.get(0)).get("sid"));
        assertEquals(String.format("%s_%s_%s_%s", chromosome, start, reference, alternate), storedVariant.get("_id"));
        assertEquals(chromosome, storedVariant.get("chr"));
        assertEquals(start, storedVariant.get("start"));
        assertEquals(end, storedVariant.get("end"));
        assertEquals(reference, storedVariant.get("ref"));
        assertEquals(alternate, storedVariant.get("alt"));
    }

    @Test
    public void includeStatsTrueShouldIncludeStatistics() throws Exception {
        Variant variant = buildVariant("12", 3, 4, "A", "T", "fileId", "studyId");

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                mongoMappingContext);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, true, false);
        variantMongoWriter.write(Collections.singletonList(variant));

        DBCollection dbCollection = mongoOperations.getCollection(collectionName);
        assertEquals(1, dbCollection.count());
        final DBObject storedVariant = dbCollection.findOne();
        assertNotNull(storedVariant.get("st"));
    }

    @Test
    public void includeStatsFalseShouldNotIncludeStatistics() throws Exception {
        Variant variant = buildVariant("12", 3, 4, "A", "T", "fileId", "studyId");

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                mongoMappingContext);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, true);
        variantMongoWriter.write(Collections.singletonList(variant));

        DBCollection dbCollection = mongoOperations.getCollection(collectionName);
        assertEquals(1, dbCollection.count());
        final DBObject storedVariant = dbCollection.findOne();
        assertNull(storedVariant.get("st"));
    }

    @Test
    public void idsIfPresentShouldBeWrittenIntoTheVariant() throws Exception {
        Variant variant = buildVariant("12", 3, 4, "A", "T", "fileId", "studyId");
        variant.setIds(new HashSet<>(Arrays.asList("a", "b", "c")));

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                mongoMappingContext);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, true);
        variantMongoWriter.write(Collections.singletonList(variant));

        DBCollection dbCollection = mongoOperations.getCollection(collectionName);
        assertEquals(1, dbCollection.count());
        final DBObject storedVariant = dbCollection.findOne();
        assertNotNull(storedVariant.get("ids"));
    }

    @Test
    public void idsIfNotPresentShouldNotBeWrittenIntoTheVariant() throws Exception {
        Variant variant = buildVariant("12", 3, 4, "A", "T", "fileId", "studyId");

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnection,
                mongoMappingContext);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, false, true);
        variantMongoWriter.write(Collections.singletonList(variant));

        DBCollection dbCollection = mongoOperations.getCollection(collectionName);
        assertEquals(1, dbCollection.count());
        final DBObject storedVariant = dbCollection.findOne();
        assertNull(storedVariant.get("ids"));
    }

    private Variant buildVariant(String chromosome, int start, int end, String reference, String alternate,
                                 String fileId, String studyId) {
        Variant variant = new Variant(chromosome, start, end, reference, alternate);

        Map<String, VariantSourceEntry> sourceEntries = new LinkedHashMap<>();
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry(fileId, studyId);
        variantSourceEntry.setCohortStats("cohortStats",
                new VariantStats(reference, alternate, Variant.VariantType.SNV));
        sourceEntries.put("variant", variantSourceEntry);
        variant.setSourceEntries(sourceEntries);

        return variant;
    }

}
