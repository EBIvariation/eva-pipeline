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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.projections.SimplifiedVariant;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.ANNOTATION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.IDS_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.SO_ACCESSION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.XREFS_FIELD;

/**
 * Write a list of {@link Variant} into MongoDB
 * See also {@link org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter}
 */
public class VariantMongoWriter extends MongoItemWriter<Variant> {

    private static final Logger logger = LoggerFactory.getLogger(VariantMongoWriter.class);

    private final MongoOperations mongoOperations;

    private final String collection;
    private final boolean includeStats;
    private final boolean includeSamples;

    public VariantMongoWriter(String collection, MongoOperations mongoOperations, boolean includeStats,
                              boolean includeSamples) {
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        this.mongoOperations = mongoOperations;
        this.collection = collection;
        setTemplate(mongoOperations);
        this.includeStats = includeStats;
        this.includeSamples = includeSamples;

        createIndexes();
    }

    @Override
    protected void doWrite(List<? extends Variant> variants) {
        BulkWriteOperation bulk = mongoOperations.getCollection(collection).initializeUnorderedBulkOperation();
        for (Variant variant : variants) {
            String id = VariantDocument.buildVariantId(variant.getChromosome(), variant.getStart(),
                    variant.getReference(), variant.getAlternate());

            // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
            BasicDBObject query = new BasicDBObject("_id", id)
                    .append(VariantDocument.CHROMOSOME_FIELD, variant.getChromosome())
                    .append(VariantDocument.START_FIELD, variant.getStart());

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
                new BasicDBObject(VariantDocument.CHROMOSOME_FIELD, 1)
                        .append(VariantDocument.START_FIELD, 1).append(VariantDocument.END_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));

        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(VariantDocument.IDS_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));

        String filesStudyIdField = String.format("%s.%s", VariantDocument.FILES_FIELD,
                VariantSourceEntryMongo.STUDYID_FIELD);
        String filesFileIdField = String.format("%s.%s", VariantDocument.FILES_FIELD,
                VariantSourceEntryMongo.FILEID_FIELD);
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(filesStudyIdField, 1).append(filesFileIdField, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));

        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(ANNOTATION_FIELD + "." + XREFS_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(ANNOTATION_FIELD + "." + SO_ACCESSION_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
    }

    private DBObject generateUpdate(Variant variant) {
        Assert.notNull(variant, "Variant should not be null. Please provide a valid Variant object");
        logger.trace("Convert variant {} into mongo object", variant);

        BasicDBObject addToSet = new BasicDBObject();

        if (!variant.getSourceEntries().isEmpty()) {
            VariantSourceEntry variantSourceEntry = variant.getSourceEntries().values().iterator().next();

            addToSet.put(VariantDocument.FILES_FIELD, convert(variantSourceEntry));

            if (includeStats) {
                BasicDBList basicDBList = convertStatistics(variantSourceEntry);
                addToSet.put(VariantDocument.STATS_FIELD, new BasicDBObject("$each", basicDBList));
            }
        }

        if (variant.getIds() != null && !variant.getIds().isEmpty()) {
            addToSet.put(IDS_FIELD, new BasicDBObject("$each", variant.getIds()));
        }

        BasicDBObject update = new BasicDBObject();
        if (!addToSet.isEmpty()) {
            update.put("$addToSet", addToSet);
        }
        update.append("$setOnInsert", convert(variant));

        return update;
    }

    private BasicDBList convertStatistics(VariantSourceEntry variantSourceEntry) {
        List<VariantStatsMongo> variantStats = new ArrayList<>();
        for (Map.Entry<String, VariantStats> variantStatsEntry : variantSourceEntry.getCohortStats().entrySet()) {
            variantStats.add(new VariantStatsMongo(
                    variantSourceEntry.getStudyId(),
                    variantSourceEntry.getFileId(),
                    variantStatsEntry.getKey(),
                    variantStatsEntry.getValue()
            ));
        }
        return (BasicDBList) mongoOperations.getConverter().convertToMongoType(variantStats);
    }

    private DBObject convert(VariantSourceEntry variantSourceEntry) {
        VariantSourceEntryMongo variantSource = null;
        if (includeSamples) {
            variantSource = new VariantSourceEntryMongo(
                    variantSourceEntry.getFileId(),
                    variantSourceEntry.getStudyId(),
                    variantSourceEntry.getSecondaryAlternates(),
                    variantSourceEntry.getAttributes(),
                    variantSourceEntry.getFormat(),
                    variantSourceEntry.getSamplesData()
            );
        } else {
            variantSource = new VariantSourceEntryMongo(
                    variantSourceEntry.getFileId(),
                    variantSourceEntry.getStudyId(),
                    variantSourceEntry.getSecondaryAlternates(),
                    variantSourceEntry.getAttributes()
            );
        }
        return (DBObject) mongoOperations.getConverter().convertToMongoType(variantSource);
    }

    private DBObject convert(Variant variant) {
        SimplifiedVariant simplifiedVariant = new SimplifiedVariant(
                variant.getType(),
                variant.getChromosome(),
                variant.getStart(),
                variant.getEnd(),
                variant.getLength(),
                variant.getReference(),
                variant.getAlternate(),
                variant.getHgvs());
        return (DBObject) mongoOperations.getConverter().convertToMongoType(simplifiedVariant);
    }
}