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

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo;
import uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper;
import uk.ac.ebi.eva.pipeline.parameters.EVAMongoConnectionDetails;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.MongoTestDataLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.SO_ACCESSION_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.AnnotationIndexMongo.XREFS_FIELD;
import static uk.ac.ebi.eva.test.data.VepOutputContent.vepOutputContentWithExtraFields;

/**
 * {@link AnnotationInVariantMongoWriter}
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {EVAMongoConnectionDetails.class, MongoMappingContext.class, BatchTestConfiguration.class})
public class AnnotationInVariantMongoWriterTest extends MongoTestContainerHelper {

    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String VEP_VERSION = "1";

    private static final String VEP_CACHE_VERSION = "2";

    private static final String DB_NAME = "annotation-metadata-test-db";

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

    private AnnotationInVariantMongoWriter annotationInVariantMongoWriter;

    private AnnotationLineMapper annotationLineMapper;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate = batchTestConfiguration.getMongoTemplate(DB_NAME, mongoMappingContext);
        mongoTemplate.getDb().drop();
        annotationLineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);

        //prepare annotation sets
        List<AnnotationMongo> annotationSet1 = new ArrayList<>();
        List<AnnotationMongo> annotationSet2 = new ArrayList<>();
        List<AnnotationMongo> annotationSet3 = new ArrayList<>();

        String[] vepOutputLines = vepOutputContentWithExtraFields.split("\n");

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
        annotationInVariantMongoWriter = new AnnotationInVariantMongoWriter(mongoTemplate, COLLECTION_VARIANTS_NAME,
                VEP_VERSION, VEP_CACHE_VERSION);

        annotationInVariantMongoWriter.write(new Chunk(Arrays.asList(annotationSet1)));
        annotationInVariantMongoWriter.write(new Chunk(Arrays.asList(annotationSet2)));
        annotationInVariantMongoWriter.write(new Chunk(Arrays.asList(annotationSet3)));

        // and finally check that variant documents have the annotations fields
        MongoCursor<Document> cursor = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).find().iterator();

        while (cursor.hasNext()) {
            Document variant = cursor.next();
            String id = (String) variant.get("_id");

            if (id.equals("20_63360_C_T_" + VEP_VERSION + "_" + VEP_CACHE_VERSION)) {
                Document annotationField = ((List<Document>) variant.get(
                        VariantMongo.ANNOTATION_FIELD)).get(0);

                checkAnnotationFields(annotationField,
                        Arrays.asList(0.1, 0.2),
                        Arrays.asList(0.1, 0.2),
                        new TreeSet<>(Arrays.asList(1631)),
                        new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000382410",
                                "ENST00000608838")));
            }

            if (id.equals("20_63399_G_A_" + VEP_VERSION + "_" + VEP_CACHE_VERSION)) {
                Document annotationField = ((List<Document>) variant.get(
                        VariantMongo.ANNOTATION_FIELD)).get(0);

                checkAnnotationFields(annotationField,
                        Arrays.asList(0.07, 0.07),
                        Arrays.asList(0.859, 0.859),
                        new TreeSet<>(Arrays.asList(1631)),
                        new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000382410",
                                "ENST00000608838")));
            }
        }
        cursor.close();
    }

    private void checkAnnotationFields(Document annotationField, List<Double> expectedSifts,
                                       List<Double> expectedPolyphens, Set<Integer> expectedSos,
                                       Set<String> expectedXrefs) {
        List<Double> sifts = (List<Double>) annotationField.get(SIFT_FIELD);
        assertEquals(expectedSifts, sifts);

        List<Double> polyphen = (List<Double>) annotationField.get(POLYPHEN_FIELD);
        assertEquals(expectedPolyphens, polyphen);

        List<Integer> so = (List<Integer>) annotationField.get(SO_ACCESSION_FIELD);
        assertEquals(expectedSos, new TreeSet<>(so));

        List<String> geneNames = (List<String>) annotationField.get(XREFS_FIELD);
        assertEquals(expectedXrefs, new TreeSet<>(geneNames));
    }

    @Test
    public void shouldUpdateFieldsOfExistingAnnotationVersion() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);

        String[] vepOutputLines = vepOutputContentWithExtraFields.split("\n");

        List<AnnotationMongo> annotations = new ArrayList<>();
        annotations.add(annotationLineMapper.mapLine(vepOutputLines[1], 0));
        annotations.add(annotationLineMapper.mapLine(vepOutputLines[2], 0));

        // load the first annotation
        annotationInVariantMongoWriter = new AnnotationInVariantMongoWriter(mongoTemplate, COLLECTION_VARIANTS_NAME,
                VEP_VERSION, VEP_CACHE_VERSION);

        List<Document> annotationField = writeAndGetAnnotation(annotations.get(0));

        checkAnnotationFields(annotationField.get(0),
                Arrays.asList(0.1, 0.1),
                Arrays.asList(0.1, 0.1),
                new TreeSet<>(Arrays.asList(1631)),
                new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000382410")));

        // load the second annotation and check the information is updated (not overwritten)
        List<Document> annotationFieldAfter = writeAndGetAnnotation(annotations.get(1));

        checkAnnotationFields(annotationFieldAfter.get(0),
                Arrays.asList(0.1, 0.2),
                Arrays.asList(0.1, 0.2),
                new TreeSet<>(Arrays.asList(1631)),
                new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000382410",
                        "ENST00000608838")));
    }

    private List<Document> writeAndGetAnnotation(AnnotationMongo annotation) throws Exception {
        annotationInVariantMongoWriter.write(new Chunk(Arrays.asList(Collections.singletonList(annotation))));

        Document query = new Document(AnnotationMongo.START_FIELD, annotation.getStart());
        MongoCursor<Document> cursor = mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).find(query)
                .iterator();

        assertTrue(cursor.hasNext());
        Document variant = cursor.next();
        assertFalse(cursor.hasNext());

        return (List<Document>) variant.get(VariantMongo.ANNOTATION_FIELD);
    }

    @Test
    public void shouldAddAnnotationIfVersionIsNotPresent() throws Exception {
        String differentVepVersion = "different_" + VEP_VERSION;
        String differentVepCacheVersion = "different_" + VEP_CACHE_VERSION;
        AnnotationLineMapper differentVersionAnnotationLineMapper = new AnnotationLineMapper(
                differentVepVersion, differentVepCacheVersion);

        new MongoTestDataLoader(mongoTemplate, resourceLoader).restoreDumpFromFolder(MONGO_DUMP);

        String[] vepOutputLines = vepOutputContentWithExtraFields.split("\n");

        AnnotationMongo firstAnnotation = annotationLineMapper.mapLine(vepOutputLines[1], 0);
        AnnotationMongo differentVersionAnnotation = differentVersionAnnotationLineMapper.mapLine(vepOutputLines[2], 0);

        // load the first annotation
        annotationInVariantMongoWriter = new AnnotationInVariantMongoWriter(mongoTemplate, COLLECTION_VARIANTS_NAME,
                VEP_VERSION, VEP_CACHE_VERSION);

        List<Document> annotationField = writeAndGetAnnotation(firstAnnotation);

        assertEquals(1, annotationField.size());
        checkAnnotationFields(annotationField.get(0),
                Arrays.asList(0.1, 0.1),
                Arrays.asList(0.1, 0.1),
                new TreeSet<>(Arrays.asList(1631)),
                new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000382410")));

        // load the second annotation and check the information is added to the annotation array
        annotationInVariantMongoWriter = new AnnotationInVariantMongoWriter(mongoTemplate, COLLECTION_VARIANTS_NAME,
                differentVepVersion,
                differentVepCacheVersion);
        List<Document> annotationFieldAfter = writeAndGetAnnotation(differentVersionAnnotation);

        assertEquals(2, annotationFieldAfter.size());
        checkAnnotationFields(annotationField.get(0),
                Arrays.asList(0.1, 0.1),
                Arrays.asList(0.1, 0.1),
                new TreeSet<>(Arrays.asList(1631)),
                new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000382410")));

        checkAnnotationFields(annotationFieldAfter.get(1),
                Arrays.asList(0.2, 0.2),
                Arrays.asList(0.2, 0.2),
                new TreeSet<>(Arrays.asList(1631)),
                new TreeSet<>(Arrays.asList("DEFB125", "ENSG00000178591", "ENST00000608838")));
    }
}

