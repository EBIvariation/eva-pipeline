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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.DatabaseInitializationConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.test.data.GtfStaticTestData;
import uk.ac.ebi.eva.test.rules.TemporalMongoRule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.utils.MongoDBHelper.getMongoOperationsFromPipelineOptions;

/**
 * {@link GeneWriter}
 * input: a List of FeatureCoordinates to each call of `.write()`
 * output: the FeatureCoordinates get written in mongo, with at least: chromosome, start and end.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {JobOptions.class, DatabaseInitializationConfiguration.class,})
public class GeneWriterTest {

    @Rule
    public TemporalMongoRule mongoRule = new TemporalMongoRule();

    @Autowired
    private JobOptions jobOptions;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporalDatabaseName();

        MongoOperations mongoOperations = getMongoOperationsFromPipelineOptions(databaseName,
                jobOptions.getMongoConnection());

        GeneWriter geneWriter = new GeneWriter(mongoOperations, jobOptions.getDbCollectionsFeaturesName());

        GeneLineMapper lineMapper = new GeneLineMapper();
        List<FeatureCoordinates> genes = new ArrayList<>();
        for (String gtfLine : GtfStaticTestData.GTF_CONTENT.split(GtfStaticTestData.GTF_LINE_SPLIT)) {
            if (!gtfLine.startsWith(GtfStaticTestData.GTF_COMMENT_LINE)) {
                genes.add(lineMapper.mapLine(gtfLine, 0));
            }
        }
        geneWriter.write(genes);

        DBCollection genesCollection = mongoRule.getCollection(databaseName, jobOptions.getDbCollectionsFeaturesName());

        // count documents in DB and check they have region (chr + start + end)
        DBCursor cursor = genesCollection.find();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject next = cursor.next();
            assertTrue(next.get("chromosome") != null);
            assertTrue(next.get("start") != null);
            assertTrue(next.get("end") != null);
        }
        assertEquals(genes.size(), count);
    }

}
