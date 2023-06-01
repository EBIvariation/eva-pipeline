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

import com.mongodb.BasicDBList;
import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.Score;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.mongo.entity.Annotation.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.Annotation.XREFS_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.Score.SCORE_DESCRIPTION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.Score.SCORE_SCORE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.SIFT_FIELD;
import static uk.ac.ebi.eva.test.data.VepOutputContent.vepOutputContent;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.count;

/**
 * {@link AnnotationMongoWriter}
 * input: a List of Annotation to each call of `.write()`
 * output: all the Annotations get written in mongo, with at least the
 * "consequence types" annotations set
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoConnectionDetails.class, MongoMappingContext.class, TemporaryRuleConfiguration.class})
public class AnnotationMongoWriterTest {

    private static final String COLLECTION_ANNOTATIONS_NAME = "annotations";

    private static final String VEP_VERSION = "1";

    private static final String VEP_CACHE_VERSION = "2";

    @Autowired
    private MongoConnectionDetails mongoConnectionDetails;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    private AnnotationMongoWriter annotationWriter;

    private AnnotationLineMapper annotationLineMapper;

    @Before
    public void setUp() throws Exception {
        annotationLineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
    }

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        List<Annotation> annotations = new ArrayList<>();
        for (String annotLine : vepOutputContent.split("\n")) {
            annotations.add(annotationLineMapper.mapLine(annotLine, 0));
        }

        // load the annotation
        MongoOperations operations = MongoConfiguration.getMongoOperations(databaseName, mongoConnectionDetails,
                                                                           mongoMappingContext);
        annotationWriter = new AnnotationMongoWriter(operations, COLLECTION_ANNOTATIONS_NAME);
        annotationWriter.write(Collections.singletonList(annotations));

        // and finally check that documents in annotation collection have annotations
        MongoCursor<Document> cursor = mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME).find().iterator();

        int count = 0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            count++;
            Document annotation = cursor.next();
            List<Document> consequenceTypes = (List<Document>) annotation.get(CONSEQUENCE_TYPE_FIELD);
            assertNotNull(consequenceTypes);
            consequenceTypeCount += consequenceTypes.size();
        }

        assertTrue(count > 0);
        assertEquals(annotations.size(), consequenceTypeCount);
    }

    /**
     * Test that every Annotation gets written, even if the same variant receives different annotation from
     * different batches.
     *
     * @throws Exception if the annotationWriter.write fails, or the DBs cleaning fails
     */
    @Test
    public void shouldWriteAllFieldsIntoMongoDbMultipleSetsAnnotations() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        //prepare annotation sets
        List<Annotation> annotationSet1 = new ArrayList<>();
        List<Annotation> annotationSet2 = new ArrayList<>();
        List<Annotation> annotationSet3 = new ArrayList<>();

        String[] vepOutputLines = vepOutputContent.split("\n");

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 0, 2)) {
            annotationSet1.add(annotationLineMapper.mapLine(annotLine, 0));
        }

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 2, 4)) {
            annotationSet2.add(annotationLineMapper.mapLine(annotLine, 0));
        }

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 4, 7)) {
            annotationSet3.add(annotationLineMapper.mapLine(annotLine, 0));
        }

        // load the annotation
        MongoOperations operations = MongoConfiguration.getMongoOperations(databaseName, mongoConnectionDetails,
                                                                           mongoMappingContext);
        annotationWriter = new AnnotationMongoWriter(operations, COLLECTION_ANNOTATIONS_NAME);

        annotationWriter.write(Collections.singletonList(annotationSet1));
        annotationWriter.write(Collections.singletonList(annotationSet2));
        annotationWriter.write(Collections.singletonList(annotationSet3));

        // and finally check that documents in DB have the correct number of annotation
        MongoCursor<Document> cursor = mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME).find()
                .iterator();

        while (cursor.hasNext()) {
            Document annotation = cursor.next();
            String id = annotation.get("_id").toString();

            if (id.equals("20_63360_C_T") || id.equals("20_63399_G_A") || id.equals("20_63426_G_T")) {
                assertEquals(2, ((BasicDBList) annotation.get(CONSEQUENCE_TYPE_FIELD)).size());
                assertEquals(4, ((BasicDBList) annotation.get(XREFS_FIELD)).size());
            }
        }
    }

    @Test
    public void shouldWriteSubstitutionScoresIntoMongoDb() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        Annotation annotation = new Annotation("X", 1, 10, "A", "T",
                VEP_VERSION, VEP_CACHE_VERSION);

        Score siftScore = new Score(0.02, "deleterious");
        Score polyphenScore = new Score(0.846, "possibly_damaging");

        ConsequenceType consequenceType = new ConsequenceType();
        consequenceType.setSift(siftScore);
        consequenceType.setPolyphen(polyphenScore);

        annotation.addConsequenceType(consequenceType);

        MongoOperations operations = MongoConfiguration.getMongoOperations(databaseName, mongoConnectionDetails,
                                                                           mongoMappingContext);
        annotationWriter = new AnnotationMongoWriter(operations, COLLECTION_ANNOTATIONS_NAME);

        annotationWriter.write(Collections.singletonList(Collections.singletonList(annotation)));

        MongoCursor<Document> cursor = mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME).find()
                .iterator();
        while (cursor.hasNext()) {
            Document annotationField = cursor.next();
            List<Document> consequenceTypes = (List<Document>) annotationField.get(CONSEQUENCE_TYPE_FIELD);

            assertNotNull(consequenceTypes);

            LinkedHashMap consequenceTypeMap = new LinkedHashMap();
            consequenceTypes.get(0).forEach(consequenceTypeMap::put);

            Document sift = (Document) consequenceTypeMap.get(SIFT_FIELD);
            Document polyphen = (Document) consequenceTypeMap.get(POLYPHEN_FIELD);

            assertEquals(sift.getString(SCORE_DESCRIPTION_FIELD), siftScore.getDescription());
            assertEquals(sift.get(SCORE_SCORE_FIELD), siftScore.getScore());

            assertEquals(polyphen.getString(SCORE_DESCRIPTION_FIELD), polyphenScore.getDescription());
            assertEquals(polyphen.get(SCORE_SCORE_FIELD), polyphenScore.getScore());

        }
    }

    @Test
    public void indexesShouldBeCreatedInBackground() throws UnknownHostException, UnsupportedEncodingException {
        String dbName = mongoRule.getRandomTemporaryDatabaseName();
        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(dbName, mongoConnectionDetails, mongoMappingContext);
        MongoCollection<Document> dbCollection = mongoOperations.getCollection(COLLECTION_ANNOTATIONS_NAME);

        AnnotationMongoWriter writer = new AnnotationMongoWriter(mongoOperations, COLLECTION_ANNOTATIONS_NAME);

        List<Document> indexInfo = new ArrayList<>();
        dbCollection.listIndexes().forEach((Block<Document>) indexInfo::add);

        Set<String> createdIndexes = indexInfo.stream().map(index -> index.get("name").toString()).collect(Collectors.toSet());
        Set<String> expectedIndexes = new HashSet<>();
        expectedIndexes.addAll(Arrays.asList("ct.so_1", "xrefs.id_1", "_id_"));

        assertEquals(expectedIndexes, createdIndexes);

        indexInfo.stream().filter(index -> !("_id_".equals(index.get("name").toString()))).forEach(index -> assertEquals("true", index.get(MongoDBHelper.BACKGROUND_INDEX).toString()));
    }

    @Test
    public void shouldUpdateFieldsOfExistingAnnotationVersion() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        List<Annotation> annotations = new ArrayList<>();
        for (String annotLine : vepOutputContent.split("\n")) {
            annotations.add(annotationLineMapper.mapLine(annotLine, 0));
        }

        // load the annotation
        MongoOperations operations = MongoConfiguration.getMongoOperations(databaseName, mongoConnectionDetails,
                mongoMappingContext);
        annotationWriter = new AnnotationMongoWriter(operations, COLLECTION_ANNOTATIONS_NAME);
        annotationWriter.write(Collections.singletonList(annotations.subList(1, 2)));

        // check that consequence type was written in the annotation document
        MongoCollection<Document> annotCollection = mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME);
        assertEquals(1, count(annotCollection.find().iterator()));
        assertEquals(1, countConsequenceType(annotCollection.find().iterator()));
        assertEquals(3, countXref(annotCollection.find().iterator()));

        // check that consequence types were added to that document
        annotationWriter.write(Collections.singletonList(annotations.subList(2, 3)));
        assertEquals(1, count(annotCollection.find().iterator()));
        assertEquals(2, countConsequenceType(annotCollection.find().iterator()));
        assertEquals(4, countXref(annotCollection.find().iterator()));
    }

    @Test
    public void shouldAddAnnotationIfAnnotationVersionIsNotPresent() throws Exception {
        String differentVepVersion = "different_" + VEP_VERSION;
        String differentVepCacheVersion = "different_" + VEP_CACHE_VERSION;
        AnnotationLineMapper differentVersionAnnotationLineMapper = new AnnotationLineMapper(differentVepVersion,
                                                                                             differentVepCacheVersion);
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        String annotLine = vepOutputContent.split("\n")[1];
        List<List<Annotation>> firstVersionAnnotation = Collections.singletonList(Collections.singletonList(
                annotationLineMapper.mapLine(annotLine, 0)));
        List<List<Annotation>> secondVersionAnnotation = Collections.singletonList((Collections.singletonList(
                differentVersionAnnotationLineMapper.mapLine(annotLine, 0))));

        // load the annotation
        MongoOperations operations = MongoConfiguration.getMongoOperations(databaseName, mongoConnectionDetails,
                mongoMappingContext);
        annotationWriter = new AnnotationMongoWriter(operations, COLLECTION_ANNOTATIONS_NAME);
        annotationWriter.write(firstVersionAnnotation);

        // check that consequence type was written in the annotation document
        MongoCollection<Document> annotCollection = mongoRule.getCollection(databaseName, COLLECTION_ANNOTATIONS_NAME);
        assertEquals(1, annotCollection.countDocuments());
        assertEquals(1, countConsequenceType(annotCollection.find().iterator()));
        assertEquals(3, countXref(annotCollection.find().iterator()));

        // check that consequence types were added to that document
        annotationWriter.write(secondVersionAnnotation);
        assertEquals(2, annotCollection.countDocuments());
        assertEquals(2, countConsequenceType(annotCollection.find().iterator()));
        assertEquals(6, countXref(annotCollection.find().iterator()));
    }

    private int countConsequenceType(MongoCursor<Document> cursor) {
        return getArrayCount(cursor, CONSEQUENCE_TYPE_FIELD);
    }

    private int getArrayCount(MongoCursor<Document> cursor, String field) {
        int count = 0;
        while (cursor.hasNext()) {
            Document annotation = cursor.next();
            List<Document> elements = (List<Document>) annotation.get(field);
            assertNotNull(elements);
            count += elements.size();
        }
        return count;
    }

    private int countXref(MongoCursor<Document> cursor) {
        return getArrayCount(cursor, XREFS_FIELD);
    }
}
