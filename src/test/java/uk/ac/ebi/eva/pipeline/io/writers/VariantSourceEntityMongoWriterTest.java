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

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.VcfHeaderReader;
import uk.ac.ebi.eva.pipeline.jobs.steps.LoadFileStep;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.test.configuration.BaseTestConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

/**
 * {@link VariantSourceEntityMongoWriter}
 * input: a VCF
 * output: the VariantSourceEntity gets written in mongo, with at least: fname, fid, sid, sname, samp, meta, stype,
 * date, aggregation. Stats are not there because those are written by the statistics job.
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:genotyped-vcf.properties"})
@ContextConfiguration(classes = {BaseTestConfiguration.class, LoadFileStep.class})
public class VariantSourceEntityMongoWriterTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobOptions jobOptions;

    @Autowired
    private MongoConfiguration mongoConfiguration;

    private String input;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getDefaultMongoOperations(databaseName);
        DBCollection fileCollection = mongoRule.getCollection(databaseName, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        filesWriter.write(Collections.singletonList(variantSourceEntity));

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

    @Test
    public void shouldWriteSamplesWithDotsInName() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getDefaultMongoOperations(databaseName);
        DBCollection fileCollection = mongoRule.getCollection(databaseName, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        Map<String, Integer> samplesPosition = new HashMap<>();
        samplesPosition.put("EUnothing", 1);
        samplesPosition.put("NA.dot", 2);
        samplesPosition.put("JP-dash", 3);
        variantSourceEntity.setSamplesPosition(samplesPosition);

        filesWriter.write(Collections.singletonList(variantSourceEntity));

        DBCursor cursor = fileCollection.find();

        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            DBObject samples = (DBObject) next.get(VariantSourceEntity.SAMPLES_FIELD);
            Set<String> keySet = samples.keySet();

            Set<String> expectedKeySet = new TreeSet<>(Arrays.asList("EUnothing", "NA£dot", "JP-dash"));
            assertEquals(expectedKeySet, keySet);
        }
    }

    @Test
    public void shouldCreateUniqueFileIndex() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = mongoConfiguration.getDefaultMongoOperations(databaseName);
        DBCollection fileCollection = mongoRule.getCollection(databaseName, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, jobOptions.getDbCollectionsFilesName());

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        filesWriter.write(Collections.singletonList(variantSourceEntity));

        List<DBObject> indexInfo = fileCollection.getIndexInfo();

        Set<String> createdIndexes = indexInfo.stream().map(index -> index.get("name").toString())
                .collect(Collectors.toSet());
        Set<String> expectedIndexes = new HashSet<>();
        expectedIndexes.addAll(Arrays.asList("unique_file", "_id_"));
        assertEquals(expectedIndexes, createdIndexes);

        DBObject uniqueIndex = indexInfo.stream().filter(
                index -> ("unique_file".equals(index.get("name").toString()))).findFirst().get();
        assertNotNull(uniqueIndex);
        assertEquals("true", uniqueIndex.get("unique").toString());
        assertEquals("true", uniqueIndex.get("background").toString());
    }

    private VariantSourceEntity getVariantSourceEntity() {
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
        input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        jobOptions.getPipelineOptions().put(JobParametersNames.INPUT_VCF, input);
    }
}
