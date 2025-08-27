/*
 * Copyright 2024 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps.variantstats;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.VariantStatsJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.VariantStatsStepConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * Test for {@link VariantStatsStepConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-stats.properties"})
@ContextConfiguration(classes = {VariantStatsJobConfiguration.class, BatchTestConfiguration.class,
        TemporaryRuleConfiguration.class, MongoConfiguration.class})
public class VariantStatsStepTest {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String DATABASE_NAME = "variant_stats_test_db";

    private static final String STUDY_ID = "1";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Before
    public void setUp() throws Exception {
        mongoRule.getTemporaryDatabase(DATABASE_NAME).drop();
    }

    @After
    public void cleanUp() {
        mongoRule.getTemporaryDatabase(DATABASE_NAME).drop();
    }

    @Test
    public void variantStatsStepShouldCalculateAndLoadStats() throws Exception {
        mongoRule.restoreDump(getResourceUrl(MONGO_DUMP), DATABASE_NAME);
        // update one of the variant to have one object in the files field to have sid/fid null
        MongoCollection<Document> collection = mongoRule.getTemporaryDatabase(DATABASE_NAME)
                .getCollection(COLLECTION_VARIANTS_NAME);
        collection.updateOne(Filters.eq("_id", "20_60343_G_A"),
                Updates.push("files", new Document("sid", null).append("fid", null)));


        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DATABASE_NAME)
                .inputStudyId(STUDY_ID)
                .chunkSize("100")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.VARIANT_STATS_STEP, jobParameters);

        // check job completed successfully
        assertCompleted(jobExecution);
        List<Document> documents = mongoRule.getTemporaryDatabase(DATABASE_NAME).getCollection(COLLECTION_VARIANTS_NAME)
                .find().into(new ArrayList<>());
        Assert.assertTrue(documents.size() == 300);
        // assert all statistics are calculated for all documents
        Assert.assertTrue(documents.stream().allMatch(doc -> doc.containsKey("st")));

        // assert statistics for the variant with 20_61098_C_T
        ArrayList<Document> variantStatsList = documents.stream().filter(doc -> doc.get("_id").equals("20_61098_C_T"))
                .findFirst().get().get("st", ArrayList.class);
        assertEquals(1, variantStatsList.size());
        Document variantStats = variantStatsList.get(0);
        Document numOfGT = (Document) variantStats.get("numGt");
        assertEquals(1290, numOfGT.get("0|0"));
        assertEquals(417, numOfGT.get("1|0"));
        assertEquals(573, numOfGT.get("0|1"));
        assertEquals(224, numOfGT.get("1|1"));
        assertEquals(0.2871405780315399, variantStats.get("maf"));
        assertEquals(0.228833869099617, variantStats.get("mgf"));
        assertEquals("T", variantStats.get("mafAl"));
        assertEquals("0|1", variantStats.get("mgfGt"));
        assertEquals(0, variantStats.get("missAl"));
        assertEquals(0, variantStats.get("missGt"));
    }

}
