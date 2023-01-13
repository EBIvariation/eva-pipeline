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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE})
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {AccessionImportJobConfiguration.class, BatchTestConfiguration.class})
public class AccessionImportJobTest {
    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void importSSIdTest() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        String variantsCollectionName = "variants";
        mongoRule.insertDocuments(databaseName, variantsCollectionName, getData());

        File accessionReport = FileUtils.getResource("/input-files/accession-report/accession-report.accessioned.vcf");
        // Run the Job
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .databaseName(databaseName)
                .collectionVariantsName(variantsCollectionName)
                .inputAccessionReport(accessionReport.getAbsolutePath())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        JobTestUtils.assertCompleted(jobExecution);
        checkStoreSSIdStep(mongoRule, databaseName, variantsCollectionName);
    }

    public void checkStoreSSIdStep(TemporaryMongoRule mongoRule, String databaseName, String collection) {
        assertEquals(getData().size(), mongoRule.getCollection(databaseName, collection).countDocuments());
        MongoCursor<Document> iterator = mongoRule.getCollection(databaseName, collection).find().iterator();
        List<Document> allDocInDB = new ArrayList<>();
        while(iterator.hasNext()){
            allDocInDB.add(iterator.next());
        }

        assertEquals(20, allDocInDB.size());

        // id added to existing id
        Document docWithIdApppend = allDocInDB.stream()
                .filter(d->d.get("_id", String.class).equals("chr1_3000185_G_T")).findAny().get();
        assertEquals("ss7357768460", docWithIdApppend.get("ids", ArrayList.class).get(0));
        assertEquals("ss7357768480", docWithIdApppend.get("ids", ArrayList.class).get(1));

        // no insert for document not found
        assertTrue(allDocInDB.stream().noneMatch(d->d.get("_id",String.class).equals("chr1_3001236__TTTTTT")));

        // all documents have atleast one id
        assertTrue(allDocInDB.stream().allMatch(d->!d.get("ids",ArrayList.class).isEmpty()));

    }

    public List<String> getData() {
        String[] data = new String[]{
                "{ \"_id\" : \"chr1_3000185_G_T\", \"chr\" : \"chr1\", \"start\" : 3000185, \"alt\" : \"T\", \"ref\" : \"G\", \"ids\": [\"ss7357768460\"] }",
                "{ \"_id\" : \"chr1_3000287_A_G\", \"chr\" : \"chr1\", \"start\" : 3000287, \"alt\" : \"G\", \"ref\" : \"A\", \"ids\": [] }",
                "{ \"_id\" : \"chr1_3000325_G_T\", \"chr\" : \"chr1\", \"start\" : 3000325, \"alt\" : \"T\", \"ref\" : \"G\" }",
                "{ \"_id\" : \"chr1_3000441_T_G\", \"chr\" : \"chr1\", \"start\" : 3000441, \"alt\" : \"G\", \"ref\" : \"T\" }",
                "{ \"_id\" : \"chr1_3001188_G_A\", \"chr\" : \"chr1\", \"start\" : 3001188, \"alt\" : \"A\", \"ref\" : \"G\" }",
                "{ \"_id\" : \"chr1_3001256_G_T\", \"chr\" : \"chr1\", \"start\" : 3001256, \"alt\" : \"T\", \"ref\" : \"G\" }",
                "{ \"_id\" : \"chr1_3001490_C_A\", \"chr\" : \"chr1\", \"start\" : 3001490, \"alt\" : \"A\", \"ref\" : \"C\" }",
                "{ \"_id\" : \"chr1_3001579_A_T\", \"chr\" : \"chr1\", \"start\" : 3001579, \"alt\" : \"T\", \"ref\" : \"A\" }",
                "{ \"_id\" : \"chr1_3001645_A_C\", \"chr\" : \"chr1\", \"start\" : 3001645, \"alt\" : \"C\", \"ref\" : \"A\" }",
                "{ \"_id\" : \"chr1_3001646_A_G\", \"chr\" : \"chr1\", \"start\" : 3001646, \"alt\" : \"G\", \"ref\" : \"A\" }",
                "{ \"_id\" : \"chr1_3001648_G_C\", \"chr\" : \"chr1\", \"start\" : 3001648, \"alt\" : \"C\", \"ref\" : \"G\" }",
                "{ \"_id\" : \"chr1_3001712_C_G\", \"chr\" : \"chr1\", \"start\" : 3001712, \"alt\" : \"G\", \"ref\" : \"C\" }",
                "{\"_id\" : \"chr1_3001190_GGTCATCTTGG_\", \"chr\" : \"chr1\", \"start\" : 3001190, \"alt\" : \"\", \"ref\" : \"GGTCATCTTGG\" }",
                "{ \"_id\" : \"chr1_3001211_CTT_\", \"chr\" : \"chr1\", \"start\" : 3001211, \"alt\" : \"\", \"ref\" : \"CTT\" }",
                "{ \"_id\" : \"chr1_3001215__AAA\", \"chr\" : \"chr1\", \"start\" : 3001215, \"alt\" : \"AAA\", \"ref\" : \"\" }",
                "{ \"_id\" : \"chr1_3001237__T\", \"chr\" : \"chr1\", \"start\" : 3001237, \"alt\" : \"T\", \"ref\" : \"\" }",
                "{ \"_id\" : \"chr1_3001237__TT\", \"chr\" : \"chr1\", \"start\" : 3001237, \"alt\" : \"TT\", \"ref\" : \"\" }",
                "{ \"_id\" : \"chr1_3001237__TTT\", \"chr\" : \"chr1\", \"start\" : 3001237, \"alt\" : \"TTT\", \"ref\" : \"\" }",
                "{ \"_id\" : \"chr1_3001237__TTTT\", \"chr\" : \"chr1\", \"start\" : 3001237, \"alt\" : \"TTTT\", \"ref\" : \"\" }",
                "{ \"_id\" : \"chr1_3001237__TTTTT\", \"chr\" : \"chr1\", \"start\" : 3001237, \"alt\" : \"TTTTT\", \"ref\" : \"\" }"
    };
        return Arrays.asList(data);
    }
}
