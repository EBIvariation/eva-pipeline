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

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import java.io.IOException;
import java.net.UnknownHostException;

import static junit.framework.TestCase.*;

/**
 * {@link NonAnnotatedVariantsMongoReader}
 * input: a variants collection address
 * output: a DBObject each time `.read()` is called, with at least: chr, start, annot
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { AnnotationJob.class, AnnotationConfiguration.class})
public class NonAnnotatedVariantsMongoReaderTest {

    private static final String DOC_CHR = "chr";
    private static final String DOC_START = "start";
    private static final String DOC_ANNOT = "annot";

    @Autowired
    private JobOptions jobOptions;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void classSetup() throws UnknownHostException {
        mongoClient = new MongoClient();
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
        jobOptions.setDbName(getClass().getSimpleName());
    }

    @Test
    public void shouldReadVariantsWithoutAnnotationField() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        insertDocuments();

        NonAnnotatedVariantsMongoReader mongoItemReader = new NonAnnotatedVariantsMongoReader(jobOptions.getPipelineOptions());
        mongoItemReader.open(executionContext);

        int itemCount = 0;
        DBObject doc;
        while((doc = mongoItemReader.read()) != null) {
            itemCount++;
            assertTrue(doc.containsField(DOC_CHR));
            assertTrue(doc.containsField(DOC_START));
            assertFalse(doc.containsField(DOC_ANNOT));
        }
        assertEquals(itemCount, 1);
        mongoItemReader.close();
    }

    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(jobOptions.getDbName());
    }

    private DBCollection collection() {
        return mongoClient.getDB(jobOptions.getDbName()).getCollection(jobOptions.getDbCollectionsVariantsName());
    }

    private void insertDocuments() throws IOException {
        collection().insert(JobTestUtils.constructDbo(VariantData.getVariantWithAnnotation()));
        collection().insert(JobTestUtils.constructDbo(VariantData.getVariantWithoutAnnotation()));
    }
}
