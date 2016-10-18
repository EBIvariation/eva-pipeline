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


import com.mongodb.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.model.PopulationStatistics;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * {@link StatisticsMongoWriter}
 * input: a List of {@link PopulationStatistics} to each call of `.write()`
 * output: the FeatureCoordinates get written in mongo, with at least: chromosome, start and end.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JobOptions.class, CommonConfiguration.class})
public class StatisticsMongoWriterTest {

    @Autowired
    private JobOptions jobOptions;
    private String dbName;

    @Before
    public void setUp() throws Exception {
        dbName = getClass().getSimpleName();
        jobOptions.setDbName(dbName);
        JobTestUtils.cleanDBs(dbName);
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        List<PopulationStatistics> populationStatisticsList = buildPopulationStatsList();

        // do the actual writing
        StatisticsMongoWriter statisticsMongoWriter = new StatisticsMongoWriter(
                jobOptions.getMongoOperations(), jobOptions.getDbCollectionsStatsName());

        int n = 1;
        for (int i = 0; i < n; i++) {
            statisticsMongoWriter.write(populationStatisticsList);
        }

        // do the checks
        DB db = new MongoClient().getDB(dbName);
        DBCollection statsCollection = db.getCollection(jobOptions.getDbCollectionsStatsName());

        // count documents in DB and check they have at least the index fields (vid, sid, cid) and maf and genotypeCount
        DBCursor cursor = statsCollection.find();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject next = cursor.next();
            assertNotNull(next.get("cid"));
            assertNotNull(next.get("sid"));
            assertNotNull(next.get("vid"));
            assertNotNull(next.get("maf"));
            assertNotNull(next.get("numGt"));
        }
        assertEquals(n, count);
    }

    @Test
    public void shouldCreateIndexesInCollection() throws Exception {
        List<PopulationStatistics> populationStatisticsList = buildPopulationStatsList();

        // do the actual writing
        StatisticsMongoWriter statisticsMongoWriter = new StatisticsMongoWriter(
                jobOptions.getMongoOperations(), jobOptions.getDbCollectionsStatsName());

        statisticsMongoWriter.write(populationStatisticsList);


        // do the checks
        DB db = new MongoClient().getDB(dbName);
        DBCollection statsCollection = db.getCollection(jobOptions.getDbCollectionsStatsName());

        // check vid has an index
        assertEquals("[{ \"v\" : 1 , \"key\" : { \"_id\" : 1} , \"name\" : \"_id_\" , \"ns\" : \"" + dbName +
                ".populationStatistics\"}, { \"v\" : 1 , \"unique\" : true , \"key\" : { \"vid\" : 1 , \"sid\" : 1 , " +
                "\"cid\" : 1} , \"name\" : \"vscid\" , \"ns\" : \"" + dbName + ".populationStatistics\"}]",
                statsCollection.getIndexInfo().toString());
    }

    @Test(expected = org.springframework.dao.DuplicateKeyException.class)
    public void shouldFailIfduplicatedVidSidCid() throws Exception {
        List<PopulationStatistics> populationStatisticsList = buildPopulationStatsList();

        // do the actual writing
        StatisticsMongoWriter statisticsMongoWriter = new StatisticsMongoWriter(
                jobOptions.getMongoOperations(), jobOptions.getDbCollectionsStatsName());

        statisticsMongoWriter.write(populationStatisticsList);
        statisticsMongoWriter.write(populationStatisticsList);   // should throw
    }

    private List<PopulationStatistics> buildPopulationStatsList() throws Exception {
        String statsPath = VariantData.getPopulationStatistics();
        JsonLineMapper mapper = new JsonLineMapper();
        Map<String, Object> map = mapper.mapLine(statsPath, 0);
        PopulationStatistics populationStatistics = new PopulationStatistics(
                (String) map.get("vid"),
                (String) map.get("cid"),
                (String) map.get("sid"),
                (Double) map.get("maf"),
                (Double) map.get("mgf"),
                (String) map.get("mafAl"),
                (String) map.get("mgfGt"),
                (Integer) map.get("missAl"),
                (Integer) map.get("missGt"),
                (Map<String, Integer>) map.get("numGt"));

        return Arrays.asList(populationStatistics);
    }
}
