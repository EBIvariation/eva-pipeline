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

import com.google.common.collect.Sets;
import com.googlecode.junittoolbox.ParallelRunner;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.*;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.MongoDBHelper;
import java.net.UnknownHostException;
import java.util.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Testing {@link VariantMongoWriter}
 */
@RunWith(ParallelRunner.class)
public class VariantMongoWriterTest {

    private VariantMongoWriter variantMongoWriter;

    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceEntryConverter sourceEntryConverter;

    private Map<String, Integer> samplesIds;
    private ObjectMap objectMap = new ObjectMap();

    private final String fileId = "fileId";
    private final String studyId = "studyId";
    private final String collectionName = "variants";
    private final Map<String, Integer> samplesPosition = new LinkedHashMap<>();
    private final VariantStorageManager.IncludeSrc includeSrc = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

    private boolean includeStats = false;
    private boolean calculateStats = false;
    private boolean includeSample = true;

    @Before
    public void setUp() throws Exception {
        objectMap.put("config.db.read-preference", "primary");
    }

    @Test
    public void noVariantsNothingShouldBeWritten() throws UnknownHostException {
        String dbName = "VariantMongoWriterNoVariant";

        setConverters(samplesPosition, calculateStats, includeSample, includeSrc);

        objectMap.put("db.name", dbName);
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(objectMap);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(includeStats, collectionName, variantConverter,
                statsConverter, sourceEntryConverter, mongoOperations);
        variantMongoWriter.doWrite(Collections.EMPTY_LIST);

        assertEquals(0, dbCollection.count());
    }

    @Test
    public void allFieldsOfAVariantShouldBeWritten() throws UnknownHostException {
        String dbName = "VariantMongoWriterTestSingleVariant";

        String chromosome = "12";
        int start = 3;
        int end = 4;
        String reference = "A";
        String alternate = "T";

        Variant variant = buildVariant(chromosome, start, end, reference, alternate, fileId, studyId);

        setConverters(samplesPosition, calculateStats, includeSample, includeSrc);

        objectMap.put("db.name", dbName);
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(objectMap);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(includeStats, collectionName, variantConverter,
                statsConverter, sourceEntryConverter, mongoOperations);
        variantMongoWriter.doWrite(Collections.singletonList(variant));

        assertEquals(1, dbCollection.count());

        Variant writtenVariant = variantConverter.convertToDataModelType(dbCollection.findOne());
        assertEquals(chromosome, writtenVariant.getChromosome());
        assertEquals(start, writtenVariant.getStart());
        assertEquals(end, writtenVariant.getEnd());
        assertEquals(reference, writtenVariant.getReference());
        assertEquals(alternate, writtenVariant.getAlternate());
        assertNull(alternate, writtenVariant.getStats(studyId, fileId));
        assertNull(dbCollection.findOne().get("st"));
        assertTrue(writtenVariant.getIds().isEmpty());

        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void multipleVariantsShouldBeWrittenIntoMongoDb() throws UnknownHostException {
        String dbName = "VariantMongoWriterTestMultipleVariants";

        Variant variant1 = buildVariant("12", 3, 5, "A", "T", fileId, studyId);
        Variant variant2 = buildVariant("12", 6, 7, "C", "GG", fileId, studyId);
        Variant variant3 = buildVariant("12", 8, 9, "G", "AA", fileId, studyId);

        setConverters(samplesPosition, calculateStats, includeSample, includeSrc);

        objectMap.put("db.name", dbName);
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(objectMap);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(includeStats, collectionName, variantConverter,
                statsConverter, sourceEntryConverter, mongoOperations);
        variantMongoWriter.doWrite(Arrays.asList(variant1, variant2, variant3));

        assertEquals(3, dbCollection.count());

        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void multipleVariantsWithDifferentFileIdStatsShouldBeReferenced() throws UnknownHostException {
        boolean calculateStats = true;

        String dbName = "multipleVariantsWithDifferentFileId";

        Variant variant1 = buildVariant("12", 3, 5, "A", "T", fileId, studyId);
        Variant variant2 = buildVariant("12", 3, 5, "A", "T", "fileId2", "studyId2");

        setConverters(samplesPosition, calculateStats, includeSample, includeSrc);

        objectMap.put("db.name", dbName);
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(objectMap);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(true, collectionName, variantConverter,
                statsConverter, sourceEntryConverter, mongoOperations);
        variantMongoWriter.doWrite(Collections.singletonList(variant1));
        variantMongoWriter.doWrite(Collections.singletonList(variant2));

        assertEquals(1, dbCollection.count());
        assertNotNull(dbCollection.findOne().get("st"));
        BasicDBList basicDBList = (BasicDBList) dbCollection.findOne().get("st");

        Set<String> fileIds = new HashSet<>();
        for (Object o : basicDBList) {
            fileIds.add((String) ((BasicDBObject) o).get("fid"));
        }
        assertTrue(CollectionUtils.isEqualCollection(new HashSet<>(Arrays.asList(fileId, "fileId2")), fileIds));

        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void includeStatsTrueShouldIncludeStatistics() throws UnknownHostException {
        boolean includeStats = true;
        boolean calculateStats = true;
        boolean includeSample = false;

        String dbName = "VariantMongoWriterTestIncludeStats";

        Variant variant = buildVariant("12", 3, 4, "A", "T", fileId, studyId);

        setConverters(samplesPosition, calculateStats, includeSample, includeSrc);

        objectMap.put("db.name", dbName);
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(objectMap);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(includeStats, collectionName, variantConverter,
                statsConverter, sourceEntryConverter, mongoOperations);
        variantMongoWriter.doWrite(Collections.singletonList(variant));

        assertEquals(1, dbCollection.count());
        assertNotNull(dbCollection.findOne().get("st"));

        JobTestUtils.cleanDBs(dbName);
    }

    @Test
    public void idsShouldBeWrittenIntoTheVariant() throws UnknownHostException {
        String dbName = "VariantMongoWriterTestNonEmptyIds";

        Variant variant = buildVariant("12", 3, 4, "A", "T", fileId, studyId);
        variant.setIds(Sets.newHashSet("a", "b", "c"));

        setConverters(samplesPosition, calculateStats, includeSample, includeSrc);

        objectMap.put("db.name", dbName);
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(objectMap);
        DBCollection dbCollection = mongoOperations.getCollection(collectionName);

        variantMongoWriter = new VariantMongoWriter(includeStats, collectionName, variantConverter,
                statsConverter, sourceEntryConverter, mongoOperations);
        variantMongoWriter.doWrite(Collections.singletonList(variant));

        assertEquals(1, dbCollection.count());
        Variant writtenVariant = variantConverter.convertToDataModelType(dbCollection.findOne());

        assertFalse(writtenVariant.getIds().isEmpty());

        JobTestUtils.cleanDBs(dbName);
    }

    private Variant buildVariant(String chromosome, int start, int end, String reference, String alternate,
                                 String fileId, String studyId){
        Variant variant = new Variant(chromosome, start, end, reference, alternate);

        Map<String, VariantSourceEntry> sourceEntries = new LinkedHashMap<>();
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry(fileId, studyId);
        variantSourceEntry.setCohortStats("cohortStats", new VariantStats(reference, alternate, Variant.VariantType.SNV));
        sourceEntries.put("variant", variantSourceEntry);
        variant.setSourceEntries(sourceEntries);

        return variant;
    }

    private void setConverters(Map<String, Integer> samplesPosition, boolean calculateStats, boolean includeSample,
                               VariantStorageManager.IncludeSrc includeSrc) {
        if (samplesIds == null || samplesIds.isEmpty()) {
            samplesIds = samplesPosition;
        }

        statsConverter = calculateStats ? new DBObjectToVariantStatsConverter() : null;
        DBObjectToSamplesConverter sampleConverter = includeSample ? new DBObjectToSamplesConverter(true, samplesIds) : null;
        sourceEntryConverter = new DBObjectToVariantSourceEntryConverter(includeSrc, sampleConverter);
        variantConverter = new DBObjectToVariantConverter(null, null);
    }
}
