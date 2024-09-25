package uk.ac.ebi.eva.pipeline.io.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.pipeline.io.readers.VariantStatsReader;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VariantStatsWriter implements ItemWriter<VariantDocument> {
    private static final Logger logger = LoggerFactory.getLogger(VariantStatsWriter.class);
    private DatabaseParameters databaseParameters;
    private MongoTemplate mongoTemplate;

    public VariantStatsWriter(DatabaseParameters databaseParameters, MongoTemplate mongoTemplate) {
        this.databaseParameters = databaseParameters;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends VariantDocument> variants) {
        Map<String, Integer> filesIdNumberOfSamplesMap = VariantStatsReader.getFilesIdAndNumberOfSamplesMap();
        if (filesIdNumberOfSamplesMap.isEmpty()) {
            // No new stats would have been calculated, no need to write anything
            return;
        }

        variants = variants.stream()
                .filter(v -> v.getVariantStatsMongo() != null && !v.getVariantStatsMongo().isEmpty())
                .collect(Collectors.toList());

        if (!variants.isEmpty()) {
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, VariantDocument.class,
                    databaseParameters.getCollectionVariantsName());

            for (VariantDocument variant : variants) {
                Query query = new Query(Criteria.where("_id").is(variant.getId()));
                Update update = new Update();
                update.set("st", variant.getVariantStatsMongo());

                bulkOperations.updateOne(query, update);
            }

            bulkOperations.execute();
        }
    }

}


