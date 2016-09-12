package uk.ac.ebi.eva.pipeline.io.writers;

import com.mongodb.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationConfig;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.io.mappers.VariantAnnotationLineMapper;
import uk.ac.ebi.eva.pipeline.jobs.VariantAnnotConfiguration;
import uk.ac.ebi.eva.test.data.VepOutputContent;
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantAnnotConfiguration.class, AnnotationConfig.class})
public class VariantAnnotationWriterTest {

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Test
    public void variantAnnotationWriterShouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbName = variantJobsArgs.getPipelineOptions().getString("db.name");
        String dbCollectionVariantsName = variantJobsArgs.getDbCollectionsVariantsName();
        JobTestUtils.cleanDBs(dbName);

        // first do a mock of a "variants" collection, with just the _id
        VariantAnnotationLineMapper lineMapper = new VariantAnnotationLineMapper();
        List<VariantAnnotation> annotations = new ArrayList<>();
        for (String annotLine : VepOutputContent.vepOutputContent.split("\n")) {
            annotations.add(lineMapper.mapLine(annotLine, 0));
        }
        MongoClient mongoClient = new MongoClient();
        DBCollection variants =
                mongoClient.getDB(dbName).getCollection(dbCollectionVariantsName);
        DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();

        Set<String> uniqueIdsLoaded = new HashSet<>();
        for (VariantAnnotation annotation : annotations) {
            String id = MongoDBHelper.buildStorageId(
                    annotation.getChromosome(),
                    annotation.getStart(),
                    annotation.getReferenceAllele(),
                    annotation.getAlternativeAllele());

            if (!uniqueIdsLoaded.contains(id)){
                variants.insert(new BasicDBObject("_id", id));
                uniqueIdsLoaded.add(id);
            }

        }

        // now, load the annotation
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(variantJobsArgs.getPipelineOptions());
        VariantAnnotationMongoItemWriter annotationWriter = new VariantAnnotationMongoItemWriter(mongoOperations, dbCollectionVariantsName);

        annotationWriter.write(annotations);

        // and finally check that documents in DB have annotation (only consequence type)
        DBCursor cursor = variants.find();

        int cnt=0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            VariantAnnotation annot = converter.convertToDataModelType((DBObject)cursor.next().get("annot"));
            assertTrue(annot.getConsequenceTypes() != null);
            consequenceTypeCount += annot.getConsequenceTypes().size();
        }
        assertTrue(cnt>0);
        assertEquals(annotations.size(), consequenceTypeCount);
    }

}
