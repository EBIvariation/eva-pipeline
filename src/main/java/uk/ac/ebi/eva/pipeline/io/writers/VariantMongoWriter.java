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
package uk.ac.ebi.eva.pipeline.io.writers;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantStatsConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;

import java.util.List;

/**
 * @author Diego Poggioli
 *         <p>
 *         Write a list of {@link Variant} into MongoDB
 *         See also {@link org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter}
 */
public class VariantMongoWriter extends MongoItemWriter<Variant> {
    private static final Logger logger = LoggerFactory.getLogger(VariantMongoWriter.class);

    private boolean includeStats;
    private MongoOperations mongoOperations;
    private String collection;

    private BulkWriteOperation bulk;
    private int currentBulkSize = 0;

    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceEntryConverter sourceEntryConverter;

    public VariantMongoWriter(boolean includeStats, String collection,
                              DBObjectToVariantConverter variantConverter, DBObjectToVariantStatsConverter statsConverter,
                              DBObjectToVariantSourceEntryConverter sourceEntryConverter, MongoOperations mongoOperations) {
        this.includeStats = includeStats;
        this.collection = collection;
        this.variantConverter = variantConverter;
        this.statsConverter = statsConverter;
        this.sourceEntryConverter = sourceEntryConverter;
        this.mongoOperations = mongoOperations;

        resetBulk();
    }

    @Override
    protected void doWrite(List<? extends Variant> variants) {
        for (Variant variant : variants) {
            variant.setAnnotation(null);
            String id = variantConverter.buildStorageId(variant);

            VariantSourceEntry variantSourceEntry = variant.getSourceEntries().entrySet().iterator().next().getValue();

            // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
            BasicDBObject query = new BasicDBObject("_id", id)
                    .append(DBObjectToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome())
                    .append(DBObjectToVariantConverter.START_FIELD, variant.getStart());

            BasicDBObject addToSet = new BasicDBObject()
                    .append(DBObjectToVariantConverter.FILES_FIELD,
                            sourceEntryConverter.convertToStorageType(variantSourceEntry));

            if (includeStats) {
                List<DBObject> sourceEntryStats = statsConverter.convertCohortsToStorageType(variantSourceEntry.getCohortStats(),
                        variantSourceEntry.getStudyId(), variantSourceEntry.getFileId());
                addToSet.put(DBObjectToVariantConverter.STATS_FIELD, new BasicDBObject("$each", sourceEntryStats));
            }

            if (variant.getIds() != null && !variant.getIds().isEmpty()) {
                addToSet.put(DBObjectToVariantConverter.IDS_FIELD, new BasicDBObject("$each", variant.getIds()));
            }

            BasicDBObject update = new BasicDBObject()
                    .append("$addToSet", addToSet)
                    .append("$setOnInsert", variantConverter.convertToStorageType(variant));    // assuming variantConverter.statsConverter == null

            bulk.find(query).upsert().updateOne(update);

            currentBulkSize++;

            executeBulk();
        }
    }

    private void executeBulk() {
        if (currentBulkSize != 0) {
            logger.debug("Execute bulk. BulkSize : " + currentBulkSize);
            bulk.execute();
            resetBulk();
        }
    }

    private void resetBulk() {
        bulk = mongoOperations.getCollection(collection).initializeUnorderedBulkOperation();
        currentBulkSize = 0;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");
    }

}
