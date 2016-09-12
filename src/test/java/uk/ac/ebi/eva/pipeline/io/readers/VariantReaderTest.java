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
import uk.ac.ebi.eva.pipeline.configuration.AnnotationConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.jobs.VariantAnnotConfiguration;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.CommonUtils;

import java.io.IOException;
import java.net.UnknownHostException;

import static junit.framework.TestCase.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantAnnotConfiguration.class, AnnotationConfig.class})
public class VariantReaderTest {

    private static final String DOC_CHR = "chr";
    private static final String DOC_START = "start";
    private static final String DOC_ANNOT = "annot";

    @Autowired
    private VariantJobsArgs variantJobsArgs;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void classSetup() throws UnknownHostException {
        mongoClient = new MongoClient();
    }

    @Before
    public void setUp() throws Exception {
        variantJobsArgs.loadArgs();
    }

    @Test
    public void variantReaderShouldReadVariantsWithoutAnnotationField() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        insertDocuments();

        VariantReader mongoItemReader = new VariantReader(variantJobsArgs.getPipelineOptions());
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
    public void setDown(){
        collection().drop();
    }

    private DBCollection collection() {
        return mongoClient.getDB(variantJobsArgs.getDbName()).getCollection(variantJobsArgs.getDbCollectionsVariantsName());
    }

    private void insertDocuments() throws IOException {
        collection().insert(CommonUtils.constructDbo(VariantData.getVariantWithAnnotation()));
        collection().insert(CommonUtils.constructDbo(VariantData.getVariantWithoutAnnotation()));
    }
}
