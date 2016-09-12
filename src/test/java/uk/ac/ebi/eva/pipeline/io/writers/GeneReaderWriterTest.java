package uk.ac.ebi.eva.pipeline.io.writers;


import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
<<<<<<< HEAD
=======
import embl.ebi.variation.eva.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.configuration.InitDBConfig;
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneLineMapper;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import embl.ebi.variation.eva.pipeline.steps.writers.GeneWriter;
>>>>>>> 15d9dcd2d437c46bd24fc5e16ea5058ff22648b6
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
<<<<<<< HEAD
import uk.ac.ebi.eva.pipeline.configuration.InitDBConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.test.data.GtfStaticTestData;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
=======
import uk.ac.ebi.eva.test.data.GtfStaticTestData;
>>>>>>> 15d9dcd2d437c46bd24fc5e16ea5058ff22648b6

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantJobsArgs.class, InitDBConfig.class,})
public class GeneReaderWriterTest {

    @Autowired
    public VariantJobsArgs variantJobsArgs;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        JobTestUtils.cleanDBs(dbName);

        GeneWriter geneWriter = new GeneWriter(variantJobsArgs.getMongoOperations(), variantJobsArgs.getDbCollectionsFeaturesName());

        GeneLineMapper lineMapper = new GeneLineMapper();
        List<FeatureCoordinates> genes = new ArrayList<>();
        for (String gtfLine : GtfStaticTestData.GTF_CONTENT.split(GtfStaticTestData.GTF_LINE_SPLIT)) {
            if (!gtfLine.startsWith(GtfStaticTestData.GTF_COMMENT_LINE)) {
                genes.add(lineMapper.mapLine(gtfLine, 0));
            }
        }
        geneWriter.write(genes);

        MongoClient mongoClient = new MongoClient();
        DBCollection genesCollection = mongoClient.getDB(dbName).getCollection(variantJobsArgs.getDbCollectionsFeaturesName());

        // count documents in DB and check they have region (chr + start + end)
        DBCursor cursor = genesCollection.find();

        int count = 0;
        while (cursor.hasNext()) {
            count++;
            DBObject next = cursor.next();
            assertTrue(next.get("chromosome") != null);
            assertTrue(next.get("start") != null);
            assertTrue(next.get("end") != null);
        }
        assertEquals(genes.size(), count);
    }

}
