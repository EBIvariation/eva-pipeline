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
package uk.ac.ebi.eva.pipeline.io.writers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.opencb.biodata.models.variant.Variant;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.pipeline.model.converters.data.VariantToMongoDbObjectConverter;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.MongoConnection;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Testing {@link VariantMongoWriter}
 */
public class VariantMongoWriterTest {

    private static final List<? extends Variant> EMPTY_LIST = new ArrayList<>();

    private VariantMongoWriter variantMongoWriter;
    private VariantToMongoDbObjectConverter variantToMongoDbObjectConverter =
            Mockito.mock(VariantToMongoDbObjectConverter.class);
    private final String collectionName = "variants";
    private MongoConnection connection;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Before
    public void setUp() throws Exception {
        connection = new MongoConnection();
        connection.setReadPreference("primary");
    }

    @Test
    public void noVariantsNothingShouldBeWritten() throws UnknownHostException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperations(dbName, connection);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, variantToMongoDbObjectConverter);
        variantMongoWriter.doWrite(EMPTY_LIST);

        assertEquals(0, dbCollection.count());
    }

    @Test
    public void variantsShouldBeWrittenIntoMongoDb() throws UnknownHostException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();

        Variant variant = Mockito.mock(Variant.class);
        when(variant.getChromosome()).thenReturn("1").thenReturn("2").thenReturn("3");
        when(variant.getStart()).thenReturn(1).thenReturn(2).thenReturn(3);
        when(variant.getReference()).thenReturn("A").thenReturn("B");
        when(variant.getAlternate()).thenReturn("B").thenReturn("C");

        MongoOperations mongoOperations = MongoDBHelper.getMongoOperations(dbName, connection);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        BasicDBObject dbObject = new BasicDBObject();

        when(variantToMongoDbObjectConverter.convert(any(Variant.class))).thenReturn(dbObject).thenReturn(dbObject);

        variantMongoWriter = new VariantMongoWriter(collectionName, mongoOperations, variantToMongoDbObjectConverter);
        variantMongoWriter.doWrite(Arrays.asList(variant, variant));

        assertEquals(2, dbCollection.count());
    }

}
