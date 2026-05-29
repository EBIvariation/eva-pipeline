/*
 * Copyright 2023 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.configuration.jobs;

import com.mongodb.client.MongoCursor;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_ACCESSION_IMPORT_JOB;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {AccessionImportJobConfiguration.class, BatchTestConfiguration.class})
public class AccessionImportJobTest extends MongoTestContainerHelper {
    private static final String DB_NAME = "accession-import-test-db";

    @Autowired
    @Qualifier(JOB_ACCESSION_IMPORT_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

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
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void importSSIdTest() throws Exception {
        String variantsCollectionName = "variants";
        mongoTemplate.getDb().getCollection(variantsCollectionName).insertMany(getData());

        File accessionReport = FileUtils.getResource("/input-files/accession-report/accession-report.accessioned.vcf");
        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(DB_NAME)
                .collectionVariantsName(variantsCollectionName)
                .inputAccessionReport(accessionReport.getAbsolutePath())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        JobTestUtils.assertCompleted(jobExecution);
        checkStoreSSIdStep(variantsCollectionName);
    }

    public void checkStoreSSIdStep(String collection) {
        assertEquals(getData().size(), mongoTemplate.getDb().getCollection(collection).countDocuments());
        MongoCursor<Document> iterator = mongoTemplate.getDb().getCollection(collection).find().iterator();
        List<Document> allDocInDB = new ArrayList<>();
        while (iterator.hasNext()) {
            allDocInDB.add(iterator.next());
        }

        assertEquals(21, allDocInDB.size());

        // id added to existing id
        Document docWithIdApppend = allDocInDB.stream()
                .filter(d -> d.get("_id", String.class).equals("chr1_3000185_G_T")).findAny().get();
        assertEquals("ss7357768460", docWithIdApppend.get("ids", ArrayList.class).get(0));
        assertEquals("ss7357768480", docWithIdApppend.get("ids", ArrayList.class).get(1));

        // no insert for document not found
        assertTrue(allDocInDB.stream().noneMatch(d -> d.get("_id", String.class).equals("chr1_3001236__TTTTTT")));

        // all documents have atleast one id
        assertTrue(allDocInDB.stream().allMatch(d -> !d.get("ids", ArrayList.class).isEmpty()));

    }

    public List<Document> getData() {
        return List.of(
                new Document("_id", "chr1_3000185_G_T")
                        .append("chr", "chr1")
                        .append("start", 3000185)
                        .append("alt", "T")
                        .append("ref", "G")
                        .append("ids", List.of("ss7357768460")),

                new Document("_id", "chr1_3000287_A_G")
                        .append("chr", "chr1")
                        .append("start", 3000287)
                        .append("alt", "G")
                        .append("ref", "A")
                        .append("ids", List.of()),

                new Document("_id", "chr1_3000325_G_T")
                        .append("chr", "chr1")
                        .append("start", 3000325)
                        .append("alt", "T")
                        .append("ref", "G"),

                new Document("_id", "chr1_3000441_T_G")
                        .append("chr", "chr1")
                        .append("start", 3000441)
                        .append("alt", "G")
                        .append("ref", "T"),

                new Document("_id", "chr1_3001188_G_A")
                        .append("chr", "chr1")
                        .append("start", 3001188)
                        .append("alt", "A")
                        .append("ref", "G"),

                new Document("_id", "chr1_3001256_G_T")
                        .append("chr", "chr1")
                        .append("start", 3001256)
                        .append("alt", "T")
                        .append("ref", "G"),

                new Document("_id", "chr1_3001490_C_A")
                        .append("chr", "chr1")
                        .append("start", 3001490)
                        .append("alt", "A")
                        .append("ref", "C"),

                new Document("_id", "chr1_3001579_A_T")
                        .append("chr", "chr1")
                        .append("start", 3001579)
                        .append("alt", "T")
                        .append("ref", "A"),

                new Document("_id", "chr1_3001645_A_C")
                        .append("chr", "chr1")
                        .append("start", 3001645)
                        .append("alt", "C")
                        .append("ref", "A"),

                new Document("_id", "chr1_3001646_A_G")
                        .append("chr", "chr1")
                        .append("start", 3001646)
                        .append("alt", "G")
                        .append("ref", "A"),

                new Document("_id", "chr1_3001648_G_C")
                        .append("chr", "chr1")
                        .append("start", 3001648)
                        .append("alt", "C")
                        .append("ref", "G"),

                new Document("_id", "chr1_3001712_C_G")
                        .append("chr", "chr1")
                        .append("start", 3001712)
                        .append("alt", "G")
                        .append("ref", "C"),

                new Document("_id", "chr1_3001190_GGTCATCTTGG_")
                        .append("chr", "chr1")
                        .append("start", 3001190)
                        .append("alt", "")
                        .append("ref", "GGTCATCTTGG"),

                new Document("_id", "chr1_3001211_CTT_")
                        .append("chr", "chr1")
                        .append("start", 3001211)
                        .append("alt", "")
                        .append("ref", "CTT"),

                new Document("_id", "chr1_3001215__AAA")
                        .append("chr", "chr1")
                        .append("start", 3001215)
                        .append("alt", "AAA")
                        .append("ref", ""),

                new Document("_id", "chr1_3001237__T")
                        .append("chr", "chr1")
                        .append("start", 3001237)
                        .append("alt", "T")
                        .append("ref", ""),

                new Document("_id", "chr1_3001237__TT")
                        .append("chr", "chr1")
                        .append("start", 3001237)
                        .append("alt", "TT")
                        .append("ref", ""),

                new Document("_id", "chr1_3001237__TTT")
                        .append("chr", "chr1")
                        .append("start", 3001237)
                        .append("alt", "TTT")
                        .append("ref", ""),

                new Document("_id", "chr1_3001237__TTTT")
                        .append("chr", "chr1")
                        .append("start", 3001237)
                        .append("alt", "TTTT")
                        .append("ref", ""),

                new Document("_id", "chr1_3001237__TTTTT")
                        .append("chr", "chr1")
                        .append("start", 3001237)
                        .append("alt", "TTTTT")
                        .append("ref", ""),

                new Document("_id", "1_100_CAG_")
                        .append("chr", "1")
                        .append("start", 100)
                        .append("alt", "")
                        .append("ref", "CAG")
        );
    }
}
