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
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
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

import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.SO_ACCESSION_FIELD;
import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.XREFS_FIELD;
import static uk.ac.ebi.eva.test.data.VepOutputContent.vepOutputContentWithExtraFields;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResourceUrl;

/**
 * {@link AnnotationInVariantMongoWriter}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoConnection.class, MongoMappingContext.class})
public class AnnotationInVariantMongoWriterTest {
    private static final String MONGO_DUMP = "/dump/VariantStatsConfigurationTest_vl";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String VEP_VERSION = "1";

    private static final String VEP_CACHE_VERSION = "1";

    @Autowired
    private MongoConnection mongoConnection;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    private AnnotationInVariantMongoWriter annotationInVariantMongoWriter;

    private uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper AnnotationLineMapper;

    @Before
    public void setUp() throws Exception {
        AnnotationLineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
    }

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String databaseName = mongoRule.restoreDumpInTemporaryDatabase(getResourceUrl(MONGO_DUMP));

        //prepare annotation sets
        List<Annotation> annotationSet1 = new ArrayList<>();
        List<Annotation> annotationSet2 = new ArrayList<>();
        List<Annotation> annotationSet3 = new ArrayList<>();

        String[] vepOutputLines = vepOutputContentWithExtraFields.split("\n");

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 0, 2)) {
            annotationSet1.add(AnnotationLineMapper.mapLine(annotLine, 0));
        }

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 2, 4)) {
            annotationSet2.add(AnnotationLineMapper.mapLine(annotLine, 0));
        }

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 4, 7)) {
            annotationSet3.add(AnnotationLineMapper.mapLine(annotLine, 0));
        }

        // load the annotation
        MongoOperations operations = MongoConfiguration.getMongoOperations(databaseName, mongoConnection,
                                                                           mongoMappingContext);
        annotationInVariantMongoWriter = new AnnotationInVariantMongoWriter(operations, COLLECTION_VARIANTS_NAME,
                                                                            VEP_VERSION, VEP_CACHE_VERSION);

        annotationInVariantMongoWriter.write(annotationSet1);
        annotationInVariantMongoWriter.write(annotationSet2);
        annotationInVariantMongoWriter.write(annotationSet3);

        // and finally check that variant documents have the annotations fields
        DBCursor cursor = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).find();

        while (cursor.hasNext()) {
            DBObject variant = cursor.next();
            String id = (String) variant.get("_id");

            if (id.equals("20_63360_C_T")) {
                BasicDBObject annotationField = (BasicDBObject) ((BasicDBList) (variant).get(
                        VariantToDBObjectConverter.ANNOTATION_FIELD)).get(0);

                BasicDBList sifts = (BasicDBList) annotationField.get(SIFT_FIELD);
                assertNotNull(sifts);
                assertTrue(sifts.containsAll(Arrays.asList(0.1, 0.2)));

                BasicDBList so = (BasicDBList) annotationField.get(SO_ACCESSION_FIELD);
                assertNotNull(so);
                assertTrue(so.contains(1631));

                BasicDBList polyphen = (BasicDBList) annotationField.get(POLYPHEN_FIELD);
                assertNotNull(polyphen);
                assertTrue(polyphen.containsAll(Arrays.asList(0.1, 0.2)));

                BasicDBList geneNames = (BasicDBList) annotationField.get(XREFS_FIELD);
                assertNotNull(geneNames);
                assertTrue(geneNames.containsAll(
                        Arrays.asList("ENST00000382410", "DEFB125", "ENST00000608838", "ENSG00000178591")));
            }

            if (id.equals("20_63399_G_A")) {
                BasicDBObject annotationField = (BasicDBObject) ((BasicDBList) (variant).get(
                        VariantToDBObjectConverter.ANNOTATION_FIELD)).get(0);

                BasicDBList sifts = (BasicDBList) annotationField.get(SIFT_FIELD);
                assertNotNull(sifts);
                assertTrue(sifts.size() == 2);

                BasicDBList so = (BasicDBList) annotationField.get(SO_ACCESSION_FIELD);
                assertNotNull(so);
                assertTrue(so.size() == 1);

                BasicDBList polyphen = (BasicDBList) annotationField.get(POLYPHEN_FIELD);
                assertNotNull(polyphen);
                assertTrue(polyphen.size() == 2);

                BasicDBList geneNames = (BasicDBList) annotationField.get(XREFS_FIELD);
                assertNotNull(geneNames);
                assertTrue(geneNames.size() == 4);
            }
        }
        cursor.close();
    }

}
