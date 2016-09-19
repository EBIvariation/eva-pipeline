package uk.ac.ebi.eva.pipeline.io.writers;

import com.mongodb.*;

import org.junit.Before;
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
import uk.ac.ebi.eva.test.utils.JobTestUtils;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static uk.ac.ebi.eva.test.data.VepOutputContent.vepOutputContent;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantAnnotConfiguration.class, AnnotationConfig.class})
public class VariantAnnotationWriterTest {

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    private DBObjectToVariantAnnotationConverter converter;
    private MongoOperations mongoOperations;
    private MongoClient mongoClient;
    private VariantAnnotationMongoItemWriter annotationWriter;
    private VariantAnnotationLineMapper variantAnnotationLineMapper;

    @Test
    public void shouldWriteAllFieldsIntoMongoDb() throws Exception {
        String dbName = variantJobsArgs.getDbName();
        String dbCollectionVariantsName = variantJobsArgs.getDbCollectionsVariantsName();
        JobTestUtils.cleanDBs(dbName);

        List<VariantAnnotation> annotations = new ArrayList<>();
        for (String annotLine : vepOutputContent.split("\n")) {
            annotations.add(variantAnnotationLineMapper.mapLine(annotLine, 0));
        }
        DBCollection variants = mongoClient.getDB(dbName).getCollection(dbCollectionVariantsName);

        // first do a mock of a "variants" collection, with just the _id
        writeIdsIntoMongo(annotations, variants);

        // now, load the annotation
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(variantJobsArgs.getPipelineOptions());
        annotationWriter = new VariantAnnotationMongoItemWriter(mongoOperations, dbCollectionVariantsName);
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

    @Test
    public void shouldWriteAllFieldsIntoMongoDbMultipleSetsAnnotations() throws Exception {
        String dbName = variantJobsArgs.getDbName();
        String dbCollectionVariantsName = variantJobsArgs.getPipelineOptions().getString("db.collections.variants.name");
        JobTestUtils.cleanDBs(dbName);

        List<VariantAnnotation> annotations = new ArrayList<>();
        for (String annotLine : vepOutputContent.split("\n")) {
            annotations.add(variantAnnotationLineMapper.mapLine(annotLine, 0));
        }
        DBCollection variants = mongoClient.getDB(dbName).getCollection(dbCollectionVariantsName);

        // first do a mock of a "variants" collection, with just the _id
        writeIdsIntoMongo(annotations, variants);

        //prepare annotation sets
        List<VariantAnnotation> annotationSet1 = new ArrayList<>();
        List<VariantAnnotation> annotationSet2 = new ArrayList<>();
        List<VariantAnnotation> annotationSet3 = new ArrayList<>();

        String[] vepOutputLines = vepOutputContent.split("\n");

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 0, 2)) {
            annotationSet1.add(variantAnnotationLineMapper.mapLine(annotLine, 0));
        }

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 2, 4)) {
            annotationSet2.add(variantAnnotationLineMapper.mapLine(annotLine, 0));
        }

        for (String annotLine : Arrays.copyOfRange(vepOutputLines, 4, 7)) {
            annotationSet3.add(variantAnnotationLineMapper.mapLine(annotLine, 0));
        }

        // now, load the annotation
        String collections = variantJobsArgs.getPipelineOptions().getString("db.collections.variants.name");
        annotationWriter = new VariantAnnotationMongoItemWriter(mongoOperations, collections);

        annotationWriter.write(annotationSet1);
        annotationWriter.write(annotationSet2);
        annotationWriter.write(annotationSet3);

        // and finally check that documents in DB have the correct number of annotation
        DBCursor cursor = variants.find();

        while (cursor.hasNext()) {
            DBObject dbObject = cursor.next();
            String id = dbObject.get("_id").toString();

            VariantAnnotation annot = converter.convertToDataModelType((DBObject) dbObject.get("annot"));

            if(id.equals("20_63360_C_T") || id.equals("20_63399_G_A") || id.equals("20_63426_G_T")){
                assertEquals(2, annot.getConsequenceTypes().size());
                assertEquals(4, annot.getXrefs().size());
            }

        }
    }

    @Before
    public void setUp() throws Exception {
        converter = new DBObjectToVariantAnnotationConverter();
        mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(variantJobsArgs.getPipelineOptions());
        mongoClient = new MongoClient();
        variantAnnotationLineMapper = new VariantAnnotationLineMapper();
    }

    private void writeIdsIntoMongo(List<VariantAnnotation> annotations, DBCollection variants) throws Exception {
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
    }

}
