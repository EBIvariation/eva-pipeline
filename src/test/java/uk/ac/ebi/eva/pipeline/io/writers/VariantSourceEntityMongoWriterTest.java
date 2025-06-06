/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.VcfHeaderReader;
import uk.ac.ebi.eva.pipeline.configuration.jobs.steps.LoadFileStepConfiguration;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;
import uk.ac.ebi.eva.test.configuration.BaseTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.io.File;
import java.util.ArrayList;
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
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * {@link VariantSourceEntityMongoWriter}
 * input: a VCF
 * output: the VariantSourceEntity gets written in mongo, with at least: fname, fid, sid, sname, samp, meta, stype,
 * date, aggregation. Stats are not there because those are written by the statistics job.
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {BaseTestConfiguration.class, LoadFileStepConfiguration.class, TemporaryRuleConfiguration.class})
public class VariantSourceEntityMongoWriterTest {

    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";

    private static final String COLLECTION_FILES_NAME = "files";

    private static final String FILE_ID = "1";

    private static final String STUDY_ID = "1";

    private static final String STUDY_NAME = "small";

    private static final VariantStudy.StudyType STUDY_TYPE = VariantStudy.StudyType.COLLECTION;

    private static final VariantSource.Aggregation AGGREGATION = VariantSource.Aggregation.NONE;

    @Autowired
    private MongoConnectionDetails mongoConnectionDetails;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    private String input;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoTemplate(databaseName, mongoConnectionDetails,
                mongoMappingContext);
        MongoCollection<Document> fileCollection = mongoRule.getCollection(databaseName, COLLECTION_FILES_NAME);

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, COLLECTION_FILES_NAME);

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        filesWriter.write(Collections.singletonList(variantSourceEntity));

        MongoCursor<Document> cursor = fileCollection.find().iterator();
        int count = 0;

        while (cursor.hasNext()) {
            count++;
            Document next = cursor.next();
            assertNotNull(next.get(VariantSourceEntity.FILEID_FIELD));
            assertNotNull(next.get(VariantSourceEntity.FILENAME_FIELD));
            assertNotNull(next.get(VariantSourceEntity.STUDYID_FIELD));
            assertNotNull(next.get(VariantSourceEntity.STUDYNAME_FIELD));
            assertNotNull(next.get(VariantSourceEntity.STUDYTYPE_FIELD));
            assertNotNull(next.get(VariantSourceEntity.AGGREGATION_FIELD));
            assertNotNull(next.get(VariantSourceEntity.SAMPLES_FIELD));
            assertNotNull(next.get(VariantSourceEntity.DATE_FIELD));

            Document meta = (Document) next.get(VariantSourceEntity.METADATA_FIELD);
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
    public void shouldUpdateWhenWriteTwice() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoTemplate(databaseName, mongoConnectionDetails,
                                                                                mongoMappingContext);
        MongoCollection<Document> fileCollection = mongoRule.getCollection(databaseName, COLLECTION_FILES_NAME);

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, COLLECTION_FILES_NAME);
        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();

        filesWriter.write(Collections.singletonList(variantSourceEntity));
        assertEquals(1, fileCollection.countDocuments());
        Document storedVariant = fileCollection.find().first();
        assertEquals(2504, ((Document) storedVariant.get(VariantSourceEntity.SAMPLES_FIELD)).size());
        assertEquals("genotyped.vcf.gz", storedVariant.get(VariantSourceEntity.FILENAME_FIELD));

        variantSourceEntity.setSamplesPosition(Collections.emptyMap());
        filesWriter.write(Collections.singletonList(variantSourceEntity));
        assertEquals(1, fileCollection.countDocuments());
        storedVariant = fileCollection.find().first();
        assertEquals(0, ((Document) storedVariant.get(VariantSourceEntity.SAMPLES_FIELD)).size());
        assertEquals("genotyped.vcf.gz", storedVariant.get(VariantSourceEntity.FILENAME_FIELD));
    }

    @Test
    public void shouldWriteSamplesWithDotsInName() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoTemplate(databaseName, mongoConnectionDetails,
                mongoMappingContext);
        MongoCollection<Document> fileCollection = mongoRule.getCollection(databaseName, COLLECTION_FILES_NAME);

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, COLLECTION_FILES_NAME);

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        Map<String, Integer> samplesPosition = new HashMap<>();
        samplesPosition.put("EUnothing", 1);
        samplesPosition.put("NA.dot", 2);
        samplesPosition.put("JP-dash", 3);
        variantSourceEntity.setSamplesPosition(samplesPosition);

        filesWriter.write(Collections.singletonList(variantSourceEntity));

        MongoCursor<Document> cursor = fileCollection.find().iterator();

        while (cursor.hasNext()) {
            Document next = cursor.next();
            Document samples = (Document) next.get(VariantSourceEntity.SAMPLES_FIELD);
            Set<String> keySet = samples.keySet();

            Set<String> expectedKeySet = new TreeSet<>(Arrays.asList("EUnothing", "NA£dot", "JP-dash"));
            assertEquals(expectedKeySet, keySet);
        }
    }

    @Test
    public void shouldCreateUniqueFileIndex() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoTemplate(databaseName, mongoConnectionDetails,
                mongoMappingContext);
        MongoCollection<Document> fileCollection = mongoRule.getCollection(databaseName, COLLECTION_FILES_NAME);

        VariantSourceEntityMongoWriter filesWriter = new VariantSourceEntityMongoWriter( mongoOperations,
                COLLECTION_FILES_NAME);

        VariantSourceEntity variantSourceEntity = getVariantSourceEntity();
        filesWriter.write(Collections.singletonList(variantSourceEntity));

        List<Document> indexInfo = new ArrayList<>();
        fileCollection.listIndexes().forEach(((Block<Document>) indexInfo::add));

        Set<String> createdIndexes = indexInfo.stream().map(index -> index.get("name").toString())
                .collect(Collectors.toSet());
        Set<String> expectedIndexes = new HashSet<>();
        expectedIndexes.addAll(Arrays.asList("sid_1_fid_1_fname_1", "_id_"));
        assertEquals(expectedIndexes, createdIndexes);

        Document uniqueIndex = indexInfo.stream().filter(
                index -> ("sid_1_fid_1_fname_1".equals(index.get("name").toString())))
                        .findFirst().get();
        assertNotNull(uniqueIndex);
        assertEquals("true", uniqueIndex.get(MongoDBHelper.UNIQUE_INDEX).toString());
        assertEquals("true", uniqueIndex.get(MongoDBHelper.BACKGROUND_INDEX).toString());
    }

    private VariantSourceEntity getVariantSourceEntity() throws Exception {
        VcfHeaderReader headerReader = new VcfHeaderReader(new File(input), FILE_ID, STUDY_ID, STUDY_NAME,
                                                           STUDY_TYPE, AGGREGATION);
        headerReader.open(null);
        return headerReader.read();
    }

    @Before
    public void setUp() throws Exception {
        input = getResource(SMALL_VCF_FILE).getAbsolutePath();
    }
}
