package uk.ac.ebi.eva.pipeline.jobs.steps.tasklets;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static java.util.Arrays.asList;

public class FileStatsTasklet implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(FileStatsTasklet.class);

    private static final String KEY_NO_OF_SAMPLES = "nSamp";
    private static final String KEY_NO_OF_VARIANTS = "nVar";
    private static final String KEY_NO_OF_SNP = "nSnp";
    private static final String KEY_NO_OF_INDEL = "nIndel";
    private static final String KEY_NO_OF_PASS = "nPass";
    private static final String KEY_NO_OF_TRANSITION = "nTi";
    private static final String KEY_NO_OF_TRANSVERSION = "nTv";

    private static final Set<String> transitions = new HashSet<>(Arrays.asList("AG", "GA", "CT", "TC"));

    // Store the map of files to number of sample from the file_2_0 collection
    private static Map<String, Integer> fileIdNumberOfSamplesMap;
    // Store the map of files to their stats counts
    private static Map<String, Map<String, Integer>> fileIdCountsMap = new HashMap<>();

    private DatabaseParameters databaseParameters;
    private MongoTemplate mongoTemplate;
    private MongoCursor<Document> cursor;
    private MongoConverter converter;
    private int chunkSize;
    private String studyId;

    public FileStatsTasklet(DatabaseParameters databaseParameters, MongoTemplate mongoTemplate, String studyId, int chunkSize) {
        this.databaseParameters = databaseParameters;
        this.mongoTemplate = mongoTemplate;
        this.studyId = studyId;
        this.chunkSize = chunkSize;
        this.converter = mongoTemplate.getConverter();
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        populateFilesIdAndNumberOfSamplesMap();

        if (!fileIdNumberOfSamplesMap.isEmpty()) {
            initializeFileIdCountsMap();
            cursor = initializeCursor();
            try {
                while (cursor.hasNext()) {
                    VariantDocument variantDocument = getVariant(cursor.next());
                    processCounts(variantDocument);
                }
            } finally {
                cursor.close();
            }

            writeStatsInTheDB();
        }

        return RepeatStatus.FINISHED;
    }

    private void populateFilesIdAndNumberOfSamplesMap() {
        Bson matchStage = match(Filters.eq("sid", studyId));
        Bson projectStage = project(fields(
                computed("fid", "$fid"),
                computed("numOfSamples", new Document("$size", new Document("$objectToArray", "$samp")))
        ));
        Bson groupStage = group("$fid", sum("totalNumOfSamples", "$numOfSamples"));

        fileIdNumberOfSamplesMap = mongoTemplate.getCollection(databaseParameters.getCollectionFilesName())
                .aggregate(asList(matchStage, projectStage, groupStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap(doc -> doc.getString("_id"), doc -> doc.getInteger("totalNumOfSamples")));
    }

    private void initializeFileIdCountsMap() {
        for (String fileId : fileIdNumberOfSamplesMap.keySet()) {
            HashMap<String, Integer> countsMap = new HashMap<>();

            countsMap.put(KEY_NO_OF_SAMPLES, fileIdNumberOfSamplesMap.get(fileId));
            countsMap.put(KEY_NO_OF_VARIANTS, 0);
            countsMap.put(KEY_NO_OF_SNP, 0);
            countsMap.put(KEY_NO_OF_INDEL, 0);
            countsMap.put(KEY_NO_OF_PASS, 0);
            countsMap.put(KEY_NO_OF_TRANSITION, 0);
            countsMap.put(KEY_NO_OF_TRANSVERSION, 0);

            fileIdCountsMap.put(fileId, countsMap);
        }
    }

    private MongoCursor<Document> initializeCursor() {
        Bson query = Filters.elemMatch(VariantDocument.FILES_FIELD, Filters.eq(VariantSourceEntryMongo.STUDYID_FIELD, studyId));
        logger.info("Issuing find: {}", query);

        FindIterable<Document> statsVariantDocuments = mongoTemplate.getCollection(databaseParameters.getCollectionVariantsName())
                .find(query)
                .noCursorTimeout(true)
                .batchSize(chunkSize);

        return statsVariantDocuments.iterator();
    }

    private VariantDocument getVariant(Document variantDocument) {
        return converter.read(VariantDocument.class, new BasicDBObject(variantDocument));
    }

    private void processCounts(VariantDocument variantDocument) {
        // get all fileIds this variant belongs to
        Set<String> fileIds = variantDocument.getVariantSources().stream()
                .filter(vse -> vse.getStudyId().equals(studyId))
                .map(vse -> vse.getFileId())
                .collect(Collectors.toSet());

        boolean isSNV = variantDocument.getVariantType().equals(Variant.VariantType.SNV);
        boolean isINDEL = variantDocument.getVariantType().equals(Variant.VariantType.INDEL);

        boolean isTransition = false;
        boolean isTransversion = false;
        if (isSNV) {
            String ref = variantDocument.getReference();
            String alt = variantDocument.getAlternate();
            String refAlt = ref + alt;
            if (transitions.contains(refAlt)) {
                isTransition = true;
            } else {
                isTransversion = true;
            }
        }

        for (String fileId : fileIds) {
            Map<String, Integer> countsMap = fileIdCountsMap.get(fileId);
            countsMap.merge(KEY_NO_OF_VARIANTS, 1, Integer::sum);

            if (isSNV) {
                countsMap.merge(KEY_NO_OF_SNP, 1, Integer::sum);
            } else if (isINDEL) {
                countsMap.merge(KEY_NO_OF_INDEL, 1, Integer::sum);
            }

            if (isTransition) {
                countsMap.merge(KEY_NO_OF_TRANSITION, 1, Integer::sum);
            } else if (isTransversion) {
                countsMap.merge(KEY_NO_OF_TRANSVERSION, 1, Integer::sum);
            }

            boolean hasPass = variantDocument.getVariantSources().stream()
                    .filter(vse -> vse.getStudyId().equals(studyId) && vse.getFileId().equals(fileId))
                    .map(vse -> (vse.getAttrs() != null) ? vse.getAttrs().getOrDefault("FILTER", "") : "")
                    .allMatch(f -> f.equals("PASS"));
            if (hasPass) {
                countsMap.merge(KEY_NO_OF_PASS, 1, Integer::sum);
            }
        }
    }

    private void writeStatsInTheDB() {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                databaseParameters.getCollectionFilesName());

        for (Map.Entry<String, Map<String, Integer>> entry : fileIdCountsMap.entrySet()) {
            String fileId = entry.getKey();
            Map<String, Integer> countsMap = entry.getValue();

            Query query = new Query(Criteria.where("sid").is(studyId).and("fid").is(fileId));
            Update update = new Update();
            update.set("st", countsMap);
            bulkOperations.updateMulti(query, update);
        }

        bulkOperations.execute();
    }
}
