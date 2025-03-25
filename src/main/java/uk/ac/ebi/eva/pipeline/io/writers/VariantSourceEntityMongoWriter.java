/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * Write a list of {@link VariantSourceEntity} into MongoDB
 */
public class VariantSourceEntityMongoWriter extends MongoItemWriter<VariantSourceEntity> {

    private static final Logger logger = LoggerFactory.getLogger(VariantSourceEntityMongoWriter.class);

    private MongoOperations mongoOperations;

    private String collection;

    public VariantSourceEntityMongoWriter(MongoOperations mongoOperations, String collection) {
        super();
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");
        setCollection(collection);
        setTemplate(mongoOperations);

        this.mongoOperations = mongoOperations;
        this.collection = collection;

        createIndexes();
    }

    @Override
    protected void doWrite(List<? extends VariantSourceEntity> variantSourceEntities) {
        List<WriteModel<Document>> writes = new ArrayList<>();
        for (VariantSourceEntity variant : variantSourceEntities) {
            // include only shard keys as part of query
            Document query = new Document()
                    .append(VariantSourceEntity.STUDYID_FIELD, variant.getStudyId())
                    .append(VariantSourceEntity.FILEID_FIELD, variant.getFileId())
                    .append(VariantSourceEntity.FILENAME_FIELD, variant.getFileName());
            Document update = new Document("$set", convertToMongo(variant));
            writes.add(new UpdateOneModel<>(query, update, new UpdateOptions().upsert(true)));
        }

        if (!writes.isEmpty()) {
            logger.info("Execute bulk. BulkSize : " + writes.size());
            mongoOperations.getCollection(collection).bulkWrite(writes);
        }
    }

    private void createIndexes() {
        mongoOperations.getCollection(collection).createIndex(
                new Document(VariantSourceEntity.STUDYID_FIELD, 1).append(VariantSourceEntity.FILEID_FIELD, 1)
                        .append(VariantSourceEntity.FILENAME_FIELD, 1), new IndexOptions().background(true)
                        .unique(true));
    }

    private Document convertToMongo(VariantSourceEntity variantSourceEntity) {
        return (Document) mongoOperations.getConverter().convertToMongoType(variantSourceEntity);
    }
}
