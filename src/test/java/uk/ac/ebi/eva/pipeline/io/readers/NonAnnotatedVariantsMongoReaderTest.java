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
package uk.ac.ebi.eva.pipeline.io.readers;

import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationLoaderStepConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * {@link NonAnnotatedVariantsMongoReader}
 * input: a variants collection address
 * output: a DBObject each time `.read()` is called, with at least: chr, start, annot
 */
@RunWith(SpringRunner.class)
@ActiveProfiles("variant-annotation-mongo")
@ContextConfiguration(classes = {AnnotationJob.class, AnnotationConfiguration.class,
        AnnotationLoaderStepConfiguration.class})
public class NonAnnotatedVariantsMongoReaderTest {

    private static final String DOC_CHR = "chr";
    private static final String DOC_START = "start";
    private static final String DOC_ANNOT = "annot";

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Autowired
    private JobOptions jobOptions;

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
    }

    @Test
    public void shouldReadVariantsWithoutAnnotationField() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        String databaseName = insertDocuments(jobOptions.getDbCollectionsVariantsName());

        NonAnnotatedVariantsMongoReader mongoItemReader = new NonAnnotatedVariantsMongoReader(databaseName,
                jobOptions.getDbCollectionsVariantsName(), jobOptions.getMongoConnection());
        mongoItemReader.open(executionContext);

        int itemCount = 0;
        DBObject doc;
        while ((doc = mongoItemReader.read()) != null) {
            itemCount++;
            assertTrue(doc.containsField(DOC_CHR));
            assertTrue(doc.containsField(DOC_START));
            assertFalse(doc.containsField(DOC_ANNOT));
        }
        assertEquals(itemCount, 1);
        mongoItemReader.close();
    }

    private String insertDocuments(String collectionName) throws IOException {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        mongoRule.insert(databaseName, collectionName, VariantData.getVariantWithAnnotation());
        mongoRule.insert(databaseName, collectionName, VariantData.getVariantWithoutAnnotation());
        return databaseName;
    }
}
