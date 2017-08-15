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

package uk.ac.ebi.eva.pipeline.io.readers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.MongoConfiguration;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * {@link VariantsMongoReader}
 * input: a variants collection address
 * output: a DBObject each time `.read()` is called, with at least: chr, start, annot
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoConnection.class, MongoMappingContext.class})
public class VariantsMongoReaderTest {

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final int EXPECTED_NO_VARIANTS = 0;

    private static final int EXPECTED_NON_ANNOTATED_VARIANTS_IN_STUDY = 1;

    private static final int EXPECTED_NON_ANNOTATED_VARIANTS_IN_DB = 2;

    private static final int EXPECTED_VARIANTS_IN_STUDY = 2;

    private static final int EXPECTED_VARIANTS_IN_DB = 3;

    private static final String STUDY_ID = "7";

    private static final String FILE_ID = "5";

    private static final String ALL_IDS = "";

    private static final String VEP_VERSION = "78";

    private static final String VEP_CACHE_VERSION = "78";

    @Autowired
    private MongoConnection mongoConnection;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Test
    public void shouldReadVariantsWithoutAnnotationFieldInAStudy() throws Exception {
        checkVariantsRead(EXPECTED_NON_ANNOTATED_VARIANTS_IN_STUDY, STUDY_ID, FILE_ID, true);
    }

    @Test
    public void shouldReadVariantsWithoutAnnotationFieldInAllStudies() throws Exception {
        checkVariantsRead(EXPECTED_NON_ANNOTATED_VARIANTS_IN_DB, ALL_IDS, ALL_IDS, true);
    }

    @Test
    public void shouldReadVariantsWithoutAnnotationFieldInAllStudiesWhenNoStudySpecified() throws Exception {
        checkVariantsRead(EXPECTED_NON_ANNOTATED_VARIANTS_IN_DB, null, null, true);
    }

    @Test
    public void shouldReadVariantsInAStudy() throws Exception {
        checkVariantsRead(EXPECTED_VARIANTS_IN_STUDY, STUDY_ID, FILE_ID, false);
    }

    @Test
    public void shouldReadVariantsInAStudyWhenNoFileSpecified() throws Exception {
        checkVariantsRead(EXPECTED_VARIANTS_IN_STUDY, STUDY_ID, null, false);
    }

    @Test
    public void shouldReadVariantsInAllStudies() throws Exception {
        checkVariantsRead(EXPECTED_VARIANTS_IN_DB, ALL_IDS, ALL_IDS, false);
    }

    @Test
    public void shouldReadVariantsInAllStudiesWhenNoStudySpecified() throws Exception {
        checkVariantsRead(EXPECTED_VARIANTS_IN_DB, null, null, false);
        checkVariantsRead(EXPECTED_VARIANTS_IN_DB, null, FILE_ID, false);
    }

    @Test
    public void shouldNotReadVariantsWhenStudyDoesNotExist() throws Exception {
        checkVariantsRead(EXPECTED_NO_VARIANTS, "nonExistingStudy", null, false);
    }

    @Test
    public void shouldNotReadVariantsInAStudyWhenFileDoesNotExist() throws Exception {
        checkVariantsRead(EXPECTED_NO_VARIANTS, STUDY_ID, "nonExistingFile", false);
    }


    private void checkVariantsRead(int expectedVariants, String study, String file, boolean excludeAnnotated)
            throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        String databaseName = mongoRule.createDBAndInsertDocuments(COLLECTION_VARIANTS_NAME, Arrays.asList(
                VariantData.getVariantWithAnnotation(),
                VariantData.getVariantWithoutAnnotation(),
                VariantData.getVariantWithoutAnnotationOtherStudy()));

        MongoOperations mongoOperations = MongoConfiguration.getMongoOperations(databaseName, mongoConnection,
                                                                                mongoMappingContext);

        VariantsMongoReader mongoItemReader = new VariantsMongoReader(
                mongoOperations, COLLECTION_VARIANTS_NAME, VEP_VERSION, VEP_CACHE_VERSION, study, file,
                excludeAnnotated);
        mongoItemReader.open(executionContext);

        int itemCount = 0;
        EnsemblVariant ensemblVariant;
        while ((ensemblVariant = mongoItemReader.read()) != null) {
            itemCount++;
            assertFalse(ensemblVariant.getChr().isEmpty());
            assertNotEquals(0, ensemblVariant.getStart());
        }
        assertEquals(expectedVariants, itemCount);
        mongoItemReader.close();
    }

}
