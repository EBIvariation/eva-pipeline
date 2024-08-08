package uk.ac.ebi.eva.pipeline.io.readers;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static java.util.Arrays.asList;

public class VariantStatsReader implements ItemStreamReader<VariantDocument> {
    private static final Logger logger = LoggerFactory.getLogger(VariantStatsReader.class);

    private DatabaseParameters databaseParameters;
    private MongoTemplate mongoTemplate;
    private MongoCursor<Document> cursor;
    private MongoConverter converter;
    private int chunkSize;
    private String studyId;

    private static Map<String, Integer> filesIdNumberOfSamplesMap = new HashMap<>();

    public VariantStatsReader(DatabaseParameters databaseParameters, MongoTemplate mongoTemplate, String studyId, int chunkSize) {
        this.databaseParameters = databaseParameters;
        this.mongoTemplate = mongoTemplate;
        this.studyId = studyId;
        this.chunkSize = chunkSize;
    }

    @Override
    public VariantDocument read() {
        Document nextElement = cursor.tryNext();
        return (nextElement != null) ? getVariant(nextElement) : null;
    }

    private VariantDocument getVariant(Document variantDocument) {
        return converter.read(VariantDocument.class, new BasicDBObject(variantDocument));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        initializeReader();
    }

    public void initializeReader() {
        cursor = initializeCursor();
        converter = mongoTemplate.getConverter();
        if (filesIdNumberOfSamplesMap.isEmpty()) {
            populateFilesIdAndNumberOfSamplesMap();
        }
    }

    private MongoCursor<Document> initializeCursor() {
        Bson query = Filters.elemMatch(VariantDocument.FILES_FIELD, Filters.eq(VariantSourceEntryMongo.STUDYID_FIELD, studyId));
        logger.info("Issuing find: {}", query);

        FindIterable<Document> statsVariantDocuments = getVariants(query);
        return statsVariantDocuments.iterator();
    }

    private FindIterable<Document> getVariants(Bson query) {
        return mongoTemplate.getCollection(databaseParameters.getCollectionVariantsName())
                .find(query)
                .noCursorTimeout(true)
                .batchSize(chunkSize);
    }

    private void populateFilesIdAndNumberOfSamplesMap() {
        Bson matchStage = match(Filters.eq("sid", studyId));
        Bson projectStage = project(fields(
                computed("fid", "$fid"),
                computed("numOfSamples", new Document("$size", new Document("$objectToArray", "$samp")))
        ));
        filesIdNumberOfSamplesMap = mongoTemplate.getCollection(databaseParameters.getCollectionFilesName())
                .aggregate(asList(matchStage, projectStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap(doc -> doc.getString("fid"), doc -> doc.getInteger("numOfSamples")));
    }

    public static Map<String, Integer> getFilesIdAndNumberOfSamplesMap() {
        return filesIdNumberOfSamplesMap;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        if (cursor != null) {
            cursor.close();
        }
    }
}


