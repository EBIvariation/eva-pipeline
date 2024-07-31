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

public class StatsVariantReader implements ItemStreamReader<VariantDocument> {
    private static final Logger logger = LoggerFactory.getLogger(StatsVariantReader.class);

    private DatabaseParameters databaseParameters;
    private MongoTemplate mongoTemplate;
    private MongoCursor<Document> cursor;
    private MongoConverter converter;
    private int chunkSize;
    private String studyId;

    public StatsVariantReader(DatabaseParameters databaseParameters, MongoTemplate mongoTemplate, String studyId, int chunkSize) {
        this.databaseParameters = databaseParameters;
        this.mongoTemplate = mongoTemplate;
        this.studyId = studyId;
        this.chunkSize = chunkSize;
    }

    @Override
    public VariantDocument read() {
        Document nextElement = cursor.tryNext();
        return (nextElement != null) ? getStatsVariant(nextElement) : null;
    }

    private VariantDocument getStatsVariant(Document variantDocument) {
        return converter.read(VariantDocument.class, new BasicDBObject(variantDocument));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        initializeReader();
    }

    public void initializeReader() {
        cursor = initializeCursor();
        converter = mongoTemplate.getConverter();
    }

    private MongoCursor<Document> initializeCursor() {
        Bson query = Filters.elemMatch(VariantDocument.FILES_FIELD, Filters.eq(VariantSourceEntryMongo.STUDYID_FIELD, studyId));
        logger.info("Issuing find: {}", query);

        FindIterable<Document> statsVariantDocuments = getStatsVariants(query);
        return statsVariantDocuments.iterator();
    }

    private FindIterable<Document> getStatsVariants(Bson query) {
        return mongoTemplate.getCollection(databaseParameters.getCollectionVariantsName())
                .find(query)
                .noCursorTimeout(true)
                .batchSize(chunkSize);
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


