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
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link VariantStatsStepConfiguration}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-stats.properties"})
@ContextConfiguration(classes = {VariantStatsJobConfiguration.class, BatchTestConfiguration.class,
        TemporaryRuleConfiguration.class, MongoConfiguration.class})
public class VariantStatsStepTestNoNewStatsCalculated {
    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String DATABASE_NAME = "variant_stats_test_db";

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
    public void variantStatsStepShouldCalculateAndLoadStats_NoNewStatsCanbeCalculated() throws Exception {
        MongoCollection<Document> filesCollection = mongoRule.getCollection(DATABASE_NAME, COLLECTION_FILES_NAME);
        MongoCollection<Document> variantsCollection = mongoRule.getCollection(DATABASE_NAME, COLLECTION_VARIANTS_NAME);

        filesCollection.insertMany(Arrays.asList(
                // multiple entries for fid2 in the files collection
                new Document("sid", "sid1").append("fid", "fid2").append("fname", "fname21")
                        .append("samp", new Document("samp21", 0).append("samp22", 1)).append("samp23", 2),
                new Document("sid", "sid1").append("fid", "fid2").append("fname", "fname22")
                        .append("samp", new Document("samp31", 0).append("samp32", 1)).append("samp33", 2)
        ));

        variantsCollection.insertMany(Arrays.asList(
                new Document("_id", "chr1_11111111_A_G").append("ref", "A").append("alt", "G")
                        .append("files", Arrays.asList(
                                // should not calculate stats - fid2 has more than one entry in the files collection
                                new Document("sid", "sid1").append("fid", "fid2")
                                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                                // should not calculate stats - no entry for fid3 in files collection
                                new Document("sid", "sid1").append("fid", "fid3")
                                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                                // should not calculate stats - different study
                                new Document("sid", "sid2").append("fid", "fid1")
                                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1)))
                        ))
                        .append("st", Arrays.asList(
                                // should not calculate or add anything for sid1 and fid2 since fid2 has more than one file

                                // should not change as it belongs to different study id
                                new Document("sid", "sid2").append("fid", "fid1")
                                        .append("maf", 0.20000000298023224).append("mgf", 0.20000000298023224)
                                        .append("mafAl", "A").append("mgfGt", "0|0"),
                                // should not change as it belongs to different fid
                                new Document("sid", "sid1").append("fid", "fid3")
                                        .append("maf", 0.30000001192092896).append("mgf", 0.30000001192092896)
                                        .append("mafAl", "A").append("mgfGt", "0|0")

                        ))
        ));

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DATABASE_NAME)
                .inputStudyId("sid1")
                .chunkSize("100")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.VARIANT_STATS_STEP, jobParameters);

        // check job completed successfully
        assertCompleted(jobExecution);
        List<Document> documents = mongoRule.getTemporaryDatabase(DATABASE_NAME).getCollection(COLLECTION_VARIANTS_NAME)
                .find().into(new ArrayList<>());
        Assert.assertTrue(documents.size() == 1);

        // assert data
        ArrayList<Document> variantStatsList = documents.stream().filter(doc -> doc.get("_id").equals("chr1_11111111_A_G"))
                .findFirst().get().get("st", ArrayList.class);
        assertEquals(2, variantStatsList.size());

        // assert remained unchanged
        Document variantStatsForSid2Fid1 = variantStatsList.stream()
                .filter(st -> st.get("sid").equals("sid2") && st.get("fid").equals("fid1")).findFirst().get();
        assertEquals(0.20000000298023224, variantStatsForSid2Fid1.get("maf"));
        assertEquals(0.20000000298023224, variantStatsForSid2Fid1.get("mgf"));
        assertEquals("A", variantStatsForSid2Fid1.get("mafAl"));
        assertEquals("0|0", variantStatsForSid2Fid1.get("mgfGt"));

        // assert remained unchanged
        Document variantStatsForSid1Fid3 = variantStatsList.stream()
                .filter(st -> st.get("sid").equals("sid1") && st.get("fid").equals("fid3")).findFirst().get();
        assertEquals(0.30000001192092896, variantStatsForSid1Fid3.get("maf"));
        assertEquals(0.30000001192092896, variantStatsForSid1Fid3.get("mgf"));
        assertEquals("A", variantStatsForSid1Fid3.get("mafAl"));
        assertEquals("0|0", variantStatsForSid1Fid3.get("mgfGt"));
    }

}
