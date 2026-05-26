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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.MongoCollectionNameConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.VariantStatsJobConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.VariantStatsStepConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_VARIANT_STATS_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;

/**
 * Test for {@link VariantStatsStepConfiguration}
 */
@ExtendWith(SpringExtension.class)
@TestPropertySource({"classpath:application.properties"})
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@ContextConfiguration(classes = {VariantStatsJobConfiguration.class, BatchTestConfiguration.class,
        MongoCollectionNameConfiguration.class})
public class VariantStatsStepTest extends MongoTestContainerHelper {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String STUDY_ID = "1";

    private static final String DB_NAME = "variant-stats-test-db";

    @Autowired
    @Qualifier(JOB_VARIANT_STATS_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    ResourceLoader resourceLoader;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;


    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate = batchTestConfiguration.getMongoTemplate(DB_NAME, mongoMappingContext);
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    public void cleanUp() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void variantStatsStepShouldCalculateAndLoadStats() throws IOException {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);
        // update one of the variant to have one object in the files field to have sid/fid null
        MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME);
        collection.updateOne(Filters.eq("_id", "20_60343_G_A"),
                Updates.push("files", new Document("sid", null).append("fid", null)));

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionFilesName(COLLECTION_FILES_NAME)
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputStudyId(STUDY_ID)
                .chunkSize("100")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.VARIANT_STATS_STEP, jobParameters);

        // check job completed successfully
        assertCompleted(jobExecution);
        List<Document> documents = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME)
                .find().into(new ArrayList<>());
        assertTrue(documents.size() == 300);
        // assert all statistics are calculated for all documents
        assertTrue(documents.stream().allMatch(doc -> doc.containsKey("st")));

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
