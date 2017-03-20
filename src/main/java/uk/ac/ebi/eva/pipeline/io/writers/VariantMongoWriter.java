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

import uk.ac.ebi.eva.commons.models.converters.data.VariantSourceEntryToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;
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

    private static final String ANNOTATION_CT_SO_FIELD = "annot.ct.so";

    private static final String ANNOTATION_XREF_ID_FIELD = "annot.xrefs.id";

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

        createIndexes();
    }

    @Override
    protected void doWrite(List<? extends Variant> variants) {
        BulkWriteOperation bulk = mongoOperations.getCollection(collection).initializeUnorderedBulkOperation();
        for (Variant variant : variants) {
            String id = MongoDBHelper.buildStorageId(variant.getChromosome(), variant.getStart(),
                                                     variant.getReference(), variant.getAlternate());

            // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
            BasicDBObject query = new BasicDBObject("_id", id)
                    .append(VariantToDBObjectConverter.CHROMOSOME_FIELD, variant.getChromosome())
                    .append(VariantToDBObjectConverter.START_FIELD, variant.getStart());

            DBObject update = variantToMongoDbObjectConverter.convert(variant);

            bulk.find(query).upsert().updateOne(update);

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
                new BasicDBObject(ANNOTATION_XREF_ID_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(ANNOTATION_CT_SO_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
    }
}