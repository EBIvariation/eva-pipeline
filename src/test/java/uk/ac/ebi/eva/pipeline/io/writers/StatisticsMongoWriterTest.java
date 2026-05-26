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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.model.PopulationStatistics;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * {@link StatisticsMongoWriter}
 * input: a List of {@link PopulationStatistics} to each call of `.write()`
 * output: the FeatureCoordinates get written in mongo, with at least: chromosome, start and end.
 * <p>
 * TODO Replace MongoDBHelper with StatisticsMongoWriterConfiguration in ContextConfiguration when the class exists
 */
@DataMongoTest(excludeAutoConfiguration = MongoRepositoriesAutoConfiguration.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {MongoConnectionDetails.class, MongoMappingContext.class})
public class StatisticsMongoWriterTest extends MongoTestContainerHelper {

    private static final String COLLECTION_STATS_NAME = "populationStatistics";

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate.getDb().drop();

        // mongodb no longer creates index by default, it needs to created explicitly or enabled to be created
        // without the compound index mongodb will consider these as 2 separate documents and insert both
        IndexOperations indexOps = mongoTemplate.indexOps(COLLECTION_STATS_NAME);
        indexOps.ensureIndex(
                new CompoundIndexDefinition(
                        new Document("chr", 1)
                                .append("start", 1)
                                .append("ref", 1)
                                .append("alt", 1)
                                .append("sid", 1)
                                .append("cid", 1)
                ).unique().named("vscid")
        );
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        PopulationStatistics populationStatisticsList = buildPopulationStatsList();

        StatisticsMongoWriter statisticsMongoWriter = getStatisticsMongoWriter();

        int expectedDocumentsCount = 1;
        for (int i = 0; i < expectedDocumentsCount; i++) {
            statisticsMongoWriter.write(Chunk.of(populationStatisticsList));
        }

        // do the checks
        MongoCollection<Document> statsCollection = mongoTemplate.getDb().getCollection(COLLECTION_STATS_NAME);
        // count documents in DB and check they have at least the index fields (vid, sid, cid) and maf and genotypeCount
        MongoCursor<Document> cursor = statsCollection.find().iterator();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            Document next = cursor.next();
            assertNotNull(next.get("cid"));
            assertNotNull(next.get("sid"));
            assertNotNull(next.get("vid"));
            assertNotNull(next.get("chr"));
            assertNotNull(next.get("start"));
            assertNotNull(next.get("ref"));
            assertNotNull(next.get("alt"));
            assertNotNull(next.get("maf"));
            assertNotNull(next.get("numGt"));
        }
        assertEquals(expectedDocumentsCount, count);
    }

    @Test
    public void shouldCreateIndexesInCollection() throws Exception {
        PopulationStatistics populationStatisticsList = buildPopulationStatsList();

        StatisticsMongoWriter statisticsMongoWriter = getStatisticsMongoWriter();
        statisticsMongoWriter.write(Chunk.of(populationStatisticsList));

        // do the checks
        MongoCollection<Document> statsCollection = mongoTemplate.getDb().getCollection(COLLECTION_STATS_NAME);

        // check there is an index in chr + start + ref + alt + sid + cid
        List<Document> indexes = new ArrayList<>();
        indexes.add(new Document("v", 2)
                .append("key", new Document("_id", 1))
                .append("name", "_id_")
        );
        indexes.add(new Document("v", 2)
                .append("unique", true)
                .append("key", new Document("chr", 1)
                        .append("start", 1)
                        .append("ref", 1)
                        .append("alt", 1)
                        .append("sid", 1)
                        .append("cid", 1))
                .append("name", "vscid")
        );

        List<Document> indexInfo = statsCollection.listIndexes().into(new ArrayList<>()).stream()
                .peek(d -> d.remove("ns"))
                .collect(Collectors.toList());

        assertEquals(indexes, indexInfo);
    }

    @Test
    public void shouldFailIfduplicatedVidSidCid() throws Exception {
        PopulationStatistics populationStatisticsList = buildPopulationStatsList();

        StatisticsMongoWriter statisticsMongoWriter = getStatisticsMongoWriter();
        statisticsMongoWriter.write(Chunk.of(populationStatisticsList));
        // should throw exception when inserting the same document again
        assertThrows(BulkOperationException.class, () -> statisticsMongoWriter.write(Chunk.of(populationStatisticsList)));
    }

    private PopulationStatistics buildPopulationStatsList() throws Exception {
        String statsPath = VariantData.getPopulationStatistics();
        JsonLineMapper mapper = new JsonLineMapper();
        Map<String, Object> map = mapper.mapLine(statsPath, 0);
        PopulationStatistics populationStatistics = new PopulationStatistics(
                (String) map.get("vid"),
                (String) map.get("chr"),
                (Integer) map.get("start"),
                (String) map.get("ref"),
                (String) map.get("alt"),
                (String) map.get("cid"),
                (String) map.get("sid"),
                (Double) map.get("maf"),
                (Double) map.get("mgf"),
                (String) map.get("mafAl"),
                (String) map.get("mgfGt"),
                (Integer) map.get("missAl"),
                (Integer) map.get("missGt"),
                (Map<String, Integer>) map.get("numGt"));

        return populationStatistics;
    }

    public StatisticsMongoWriter getStatisticsMongoWriter() {
        return new StatisticsMongoWriter(mongoTemplate, COLLECTION_STATS_NAME);
    }
}
