/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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


import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.data.GtfStaticTestData;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * {@link GeneWriter}
 * input: a List of FeatureCoordinates to each call of `.write()`
 * output: the FeatureCoordinates get written in mongo, with at least: chromosome, start and end.
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoConnectionDetails.class, MongoMappingContext.class, TemporaryRuleConfiguration.class})
public class GeneWriterTest {

    private static final String COLLECTION_FEATURES_NAME = "features";

    @Autowired
    private MongoConnectionDetails mongoConnectionDetails;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        MongoOperations mongoOperations = MongoConfiguration.getMongoTemplate(databaseName, mongoConnectionDetails, mongoMappingContext);

        GeneWriter geneWriter = new GeneWriter(mongoOperations, COLLECTION_FEATURES_NAME);

        GeneLineMapper lineMapper = new GeneLineMapper();
        List<FeatureCoordinates> genes = new ArrayList<>();
        for (String gtfLine : GtfStaticTestData.GTF_CONTENT.split(GtfStaticTestData.GTF_LINE_SPLIT)) {
            if (!gtfLine.startsWith(GtfStaticTestData.GTF_COMMENT_LINE)) {
                genes.add(lineMapper.mapLine(gtfLine, 0));
            }
        }
        geneWriter.write(genes);

        MongoCollection<Document> genesCollection = mongoRule.getCollection(databaseName, COLLECTION_FEATURES_NAME);

        // count documents in DB and check they have region (chr + start + end)
        MongoCursor<Document> cursor = genesCollection.find().iterator();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            Document next = cursor.next();
            assertNotNull(next.get("chromosome"));
            assertNotNull(next.get("start"));
            assertNotNull(next.get("end"));
        }
        assertEquals(genes.size(), count);
    }

}
