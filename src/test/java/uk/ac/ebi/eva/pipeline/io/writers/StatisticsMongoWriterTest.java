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


import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.model.PopulationStats;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

/**
 * {@link StatisticsMongoWriter}
 * input: a List of {@link PopulationStats} to each call of `.write()`
 * output: the FeatureCoordinates get written in mongo, with at least: chromosome, start and end.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JobOptions.class, CommonConfiguration.class})
public class StatisticsMongoWriterTest {

    @Autowired
    public JobOptions jobOptions;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbName = jobOptions.getDbName();
        JobTestUtils.cleanDBs(dbName);

        String statsPath = VariantData.getPopulationStatsPath();
        JsonLineMapper mapper = new JsonLineMapper();
        Map<String, Object> map = mapper.mapLine(statsPath, 0);
        PopulationStats populationStats = new PopulationStats();
        populationStats.setCohortId((String)map.get("cid"));
        populationStats.setVariantId((String)map.get("vid"));
        populationStats.setStudyId((String)map.get("sid"));
        populationStats.setMaf((Double)map.get("maf"));
        populationStats.setMgf((Double)map.get("mgf"));
        populationStats.setMafAllele((String)map.get("mafAl"));
        populationStats.setMgfGenotype((String)map.get("mgfGt"));
        populationStats.setMissingAlleles((Integer) map.get("missAl"));
        populationStats.setMissingGenotypes((Integer) map.get("missGt"));
        populationStats.setGenotypeCount((Map<String, Integer>) map.get("numGt"));


        List<PopulationStats> populationStatsList = Arrays.asList(populationStats);

        // do the actual writing
        StatisticsMongoWriter statisticsMongoWriter = new StatisticsMongoWriter(
                jobOptions.getMongoOperations(), jobOptions.getDbCollectionsStatsName());

        statisticsMongoWriter.write(populationStatsList);


        // do the checks
        MongoClient mongoClient = new MongoClient();
        DBCollection statsCollection = mongoClient.getDB(dbName).getCollection(jobOptions.getDbCollectionsStatsName());

        // count documents in DB and check they have region (chr + start + end)
        DBCursor cursor = statsCollection.find();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject next = cursor.next();
            assertTrue(next.get("cid") != null);
            assertTrue(next.get("sid") != null);
            assertTrue(next.get("vid") != null);
            assertTrue(next.get("maf") != null);
            assertTrue(next.get("numGt") != null);
        }
        assertTrue(count > 0);
    }

}
