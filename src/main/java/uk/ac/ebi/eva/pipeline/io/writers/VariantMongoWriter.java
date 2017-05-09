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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.converters.data.SamplesToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantSourceEntryToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantStatsToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.List;

/**
 * Write a list of {@link Variant} into MongoDB
 * See also {@link org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter}
 */
public class VariantMongoWriter extends MongoItemWriter<Variant> {

    private static final Logger logger = LoggerFactory.getLogger(VariantMongoWriter.class);

    private static final String VARIANT_ANNOTATION_SO_FIELD = "so";

    private final MongoOperations mongoOperations;

    private final String collection;

    private VariantToDBObjectConverter variantConverter;
    private VariantStatsToDBObjectConverter statsConverter;
    private VariantSourceEntryToDBObjectConverter sourceEntryConverter;

    public VariantMongoWriter(String collection, MongoOperations mongoOperations, boolean includeStats,
                              boolean includeSamples) {
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        this.mongoOperations = mongoOperations;
        this.collection = collection;
        setTemplate(mongoOperations);

        initializeConverters(includeStats, includeSamples);
        createIndexes();
    }

    private void initializeConverters(boolean includeStats, boolean includeSamples) {
        this.statsConverter = includeStats ? new VariantStatsToDBObjectConverter() : null;
        SamplesToDBObjectConverter sampleConverter = includeSamples ? new SamplesToDBObjectConverter() : null;
        this.sourceEntryConverter = new VariantSourceEntryToDBObjectConverter(sampleConverter);
        this.variantConverter = new VariantToDBObjectConverter();
    }

    @Override
    protected void doWrite(List<? extends Variant> variants) {
        BulkWriteOperation bulk = mongoOperations.getCollection(collection).initializeUnorderedBulkOperation();
        for (Variant variant : variants) {
            String id = Variant.buildVariantId(variant.getChromosome(), variant.getStart(),
                    variant.getReference(), variant.getAlternate());

            // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
            BasicDBObject query = new BasicDBObject("_id", id)
                    .append(VariantToDBObjectConverter.CHROMOSOME_FIELD, variant.getChromosome())
                    .append(VariantToDBObjectConverter.START_FIELD, variant.getStart());

            bulk.find(query).upsert().updateOne(generateUpdate(variant));

        }

        executeBulk(bulk, variants.size());
    }

    private void executeBulk(BulkWriteOperation bulk, int currentBulkSize) {
        if (currentBulkSize != 0) {
            logger.trace("Execute bulk. BulkSize : " + currentBulkSize);
            bulk.execute();
        }
    }

    private void createIndexes() {
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(VariantToDBObjectConverter.CHROMOSOME_FIELD, 1)
                    .append(VariantToDBObjectConverter.START_FIELD, 1).append(VariantToDBObjectConverter.END_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));

        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(VariantToDBObjectConverter.IDS_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));

        String filesStudyIdField = String.format("%s.%s", VariantToDBObjectConverter.FILES_FIELD,
                                                 VariantSourceEntryToDBObjectConverter.STUDYID_FIELD);
        String filesFileIdField = String.format("%s.%s", VariantToDBObjectConverter.FILES_FIELD,
                                                 VariantSourceEntryToDBObjectConverter.FILEID_FIELD);
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(filesStudyIdField, 1).append(filesFileIdField, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));

        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(VARIANT_ANNOTATION_SO_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
    }

    private DBObject generateUpdate(Variant variant) {
        Assert.notNull(variant, "Variant should not be null. Please provide a valid Variant object");
        logger.trace("Convert variant {} into mongo object", variant);

        BasicDBObject addToSet = new BasicDBObject();

        if (!variant.getSourceEntries().isEmpty()) {
            VariantSourceEntry variantSourceEntry = variant.getSourceEntries().values().iterator().next();

            addToSet.put(VariantToDBObjectConverter.FILES_FIELD, sourceEntryConverter.convert(variantSourceEntry));

            if (statsConverter != null) {
                List<DBObject> sourceEntryStats = statsConverter.convert(variantSourceEntry);
                addToSet.put(VariantToDBObjectConverter.STATS_FIELD, new BasicDBObject("$each", sourceEntryStats));
            }
        }

        if (variant.getIds() != null && !variant.getIds().isEmpty()) {
            addToSet.put(VariantToDBObjectConverter.IDS_FIELD, new BasicDBObject("$each", variant.getIds()));
        }

        BasicDBObject update = new BasicDBObject();
        if (!addToSet.isEmpty()) {
            update.put("$addToSet", addToSet);
        }
        update.append("$setOnInsert", variantConverter.convert(variant));

        return update;
    }
}