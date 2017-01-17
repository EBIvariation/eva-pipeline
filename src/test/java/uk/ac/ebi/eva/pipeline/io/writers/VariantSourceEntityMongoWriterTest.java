/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;
import uk.ac.ebi.eva.pipeline.io.readers.VcfHeaderReader;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.configuration.BaseTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;
import static uk.ac.ebi.eva.utils.MongoDBHelper.getMongoOperations;

/**
 * {@link VariantSourceEntityMongoWriter}
 * input: a VCF
 * output: the VariantSourceEntity gets written in mongo, with at least: fname, fid, sid, sname, samp, meta, stype,
 * date, aggregation. Stats are not there because those are written by the statistics job.
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:genotyped-vcf.properties"})
@ContextConfiguration(classes = {BaseTestConfiguration.class})
public class VariantSourceEntityMongoWriterTest {

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobOptions jobOptions;

    private String input;

    private MongoOperations mongoOperations;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        mongoOperations = getMongoOperations(databaseName, jobOptions.getMongoConnection());
        DBCollection fileCollection = mongoRule.getCollection(databaseName, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        filesWriter.write(Collections.singletonList(variantSourceEntity));

        // count documents in DB and check they have region (chr + start + end)
        DBCursor cursor = fileCollection.find();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject next = cursor.next();
            assertNotNull(next.get(VariantSourceEntity.FILEID_FIELD));
            assertNotNull(next.get(VariantSourceEntity.FILENAME_FIELD));
            assertNotNull(next.get(VariantSourceEntity.STUDYID_FIELD));
            assertNotNull(next.get(VariantSourceEntity.STUDYNAME_FIELD));
            assertNotNull(next.get(VariantSourceEntity.STUDYTYPE_FIELD));
            assertNotNull(next.get(VariantSourceEntity.AGGREGATION_FIELD));
            assertNotNull(next.get(VariantSourceEntity.SAMPLES_FIELD));
            assertNotNull(next.get(VariantSourceEntity.DATE_FIELD));

            DBObject meta = (DBObject) next.get(VariantSourceEntity.METADATA_FIELD);
            assertNotNull(meta);
            assertNotNull(meta.get(VariantSourceEntity.METADATA_FILEFORMAT_FIELD));
            assertNotNull(meta.get(VariantSourceEntity.METADATA_HEADER_FIELD));
            assertNotNull(meta.get("ALT"));
            assertNotNull(meta.get("FILTER"));
            assertNotNull(meta.get("INFO"));
            assertNotNull(meta.get("FORMAT"));
        }
        assertEquals(1, count);
    }

    private VariantSourceEntity getVariantSourceEntity() throws Exception {
        VariantSource source = (VariantSource) jobOptions.getVariantOptions().get(
                VariantStorageManager.VARIANT_SOURCE);
        String fileId = source.getFileId();
        String studyId = source.getStudyId();
        String studyName = source.getStudyName();
        VariantStudy.StudyType studyType = source.getType();
        VariantSource.Aggregation aggregation = source.getAggregation();

        VcfHeaderReader headerReader = new VcfHeaderReader(new File(input), fileId, studyId, studyName,
                                                           studyType, aggregation);

        return headerReader.read();
    }

    @Before
    public void setUp() throws Exception {
        input = getResource(jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF)).getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, input);
    }
}
