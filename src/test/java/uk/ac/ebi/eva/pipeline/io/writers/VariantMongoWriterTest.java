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

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteException;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.writers.VariantWriterConfiguration;
import uk.ac.ebi.eva.pipeline.model.converters.data.VariantToMongoDbObjectConverter;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.test.configuration.BaseTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Testing {@link VariantMongoWriter}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {BaseTestConfiguration.class, VariantWriterConfiguration.class})
public class VariantMongoWriterTest {

    @Autowired
    private MongoConfiguration mongoConfiguration;

    @Autowired
    private MongoConnection mongoConnection;

    private static final List<? extends Variant> EMPTY_LIST = new ArrayList<>();

    private VariantToMongoDbObjectConverter variantToMongoDbObjectConverter =
            Mockito.mock(VariantToMongoDbObjectConverter.class);

    private final String collectionName = "variants";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Test
    public void noVariantsNothingShouldBeWritten() throws UnknownHostException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getMongoOperations(dbName, mongoConnection);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations,
                                                                       variantToMongoDbObjectConverter);
        variantMongoWriter.doWrite(EMPTY_LIST);

        assertEquals(0, dbCollection.count());
    }

    @Test
    public void variantsShouldBeWrittenIntoMongoDb() throws Exception {
        Variant variant1 = new Variant("1", 1, 2, "A", "T");
        Variant variant2 = new Variant("2", 3, 4, "C", "G");

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getMongoOperations(dbName, mongoConnection);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        BasicDBObject dbObject = new BasicDBObject();

        when(variantToMongoDbObjectConverter.convert(any(Variant.class))).thenReturn(dbObject).thenReturn(dbObject);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations,
                                                                       variantToMongoDbObjectConverter);
        variantMongoWriter.write(Collections.singletonList(variant1));
        variantMongoWriter.write(Collections.singletonList(variant2));

        assertEquals(2, dbCollection.count());
    }

    @Test
    public void indexesShouldBeCreatedInBackground() throws UnknownHostException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getMongoOperations(dbName, mongoConnection);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations,
                                                                       variantToMongoDbObjectConverter);

        List<DBObject> indexInfo = dbCollection.getIndexInfo();

        Set<String> createdIndexes = indexInfo.stream().map(index -> index.get("name").toString())
                .collect(Collectors.toSet());
        Set<String> expectedIndexes = new HashSet<>();
        expectedIndexes.addAll(Arrays.asList("annot.ct.so_1", "annot.xrefs.id_1", "chr_1_start_1_end_1",
                                             "files.sid_1_files.fid_1", "_id_", "ids_1"));
        assertEquals(expectedIndexes, createdIndexes);

        indexInfo.stream().filter(index -> !("_id_".equals(index.get("name").toString())))
                          .forEach(index -> assertEquals("true", index.get("background").toString()));
    }

    @Test
    public void testNoDuplicatesCanBeInserted() throws Exception {
        Variant variant1 = new Variant("1", 1, 2, "A", "T");

        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getMongoOperations(dbName, mongoConnection);

        BasicDBObject dbObject = new BasicDBObject();

        when(variantToMongoDbObjectConverter.convert(any(Variant.class))).thenReturn(dbObject).thenReturn(dbObject);

        VariantMongoWriter variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations,
                                                                       variantToMongoDbObjectConverter);
        variantMongoWriter.write(Collections.singletonList(variant1));

        try {
            variantMongoWriter.write(Collections.singletonList(variant1));
            fail("Should have thrown a mongo write exception due to duplicate key");
        } catch (BulkWriteException e) {
            assertTrue(e.getMessage().contains("duplicate key"));
        }
    }

}
