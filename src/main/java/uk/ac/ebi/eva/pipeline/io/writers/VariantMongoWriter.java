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
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.model.converters.data.VariantToMongoDbObjectConverter;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.List;

/**
 * Write a list of {@link Variant} into MongoDB
 * See also {@link org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter}
 */
public class VariantMongoWriter extends MongoItemWriter<Variant> {
    private static final Logger logger = LoggerFactory.getLogger(VariantMongoWriter.class);

    private static final String BACKGROUND_INDEX = "background";

    private static final String ANNOTATION_FIELD = "annot";

    private final MongoOperations mongoOperations;

    private final String collection;

    private final VariantToMongoDbObjectConverter variantToMongoDbObjectConverter;

    public VariantMongoWriter(String collection, MongoOperations mongoOperations,
                              VariantToMongoDbObjectConverter variantToMongoDbObjectConverter) {
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        this.variantToMongoDbObjectConverter = variantToMongoDbObjectConverter;
        this.mongoOperations = mongoOperations;
        this.collection = collection;
        setTemplate(mongoOperations);

        generateIndexes();
    }

    @Override
    protected void doWrite(List<? extends Variant> variants) {
        BulkWriteOperation bulk = mongoOperations.getCollection(collection).initializeUnorderedBulkOperation();
        for (Variant variant : variants) {
            String id = MongoDBHelper.buildStorageId(variant.getChromosome(), variant.getStart(),
                                                     variant.getReference(), variant.getAlternate());

            // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
            BasicDBObject query = new BasicDBObject("_id", id)
                    .append(DBObjectToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome())
                    .append(DBObjectToVariantConverter.START_FIELD, variant.getStart());

            DBObject update = variantToMongoDbObjectConverter.convert(variant);

            bulk.find(query).upsert().updateOne(update);

        }

        executeBulk(bulk, variants.size());
    }

    private void executeBulk(BulkWriteOperation bulk, int currentBulkSize) {
        if (currentBulkSize != 0) {
            logger.debug("Execute bulk. BulkSize : " + currentBulkSize);
            bulk.execute();
        }
    }

    private void generateIndexes() {
        mongoOperations.getCollection(collection).createIndex(new BasicDBObject(
                DBObjectToVariantConverter.CHROMOSOME_FIELD, 1).append(DBObjectToVariantConverter.START_FIELD, 1)
                                                                      .append(DBObjectToVariantConverter.END_FIELD, 1)
                                                                      .append(BACKGROUND_INDEX, true));
        mongoOperations.getCollection(collection).createIndex(new BasicDBObject(
                DBObjectToVariantConverter.IDS_FIELD, 1).append(BACKGROUND_INDEX, true));

        String filesStudyIdField = String.format("%s.%s", DBObjectToVariantConverter.FILES_FIELD,
                                                 DBObjectToVariantSourceEntryConverter.STUDYID_FIELD);
        String filesFileIdField = String.format("%s.%s", DBObjectToVariantConverter.FILES_FIELD,
                                                DBObjectToVariantSourceEntryConverter.FILEID_FIELD);
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(filesStudyIdField, 1).append(filesFileIdField, 1).append(BACKGROUND_INDEX, true));
        mongoOperations.getCollection(collection)
                .createIndex(new BasicDBObject("annot.xrefs.id", 1).append(BACKGROUND_INDEX, true));
        mongoOperations.getCollection(collection)
                .createIndex(new BasicDBObject(ANNOTATION_FIELD, 1).append(BACKGROUND_INDEX, true));
    }
}