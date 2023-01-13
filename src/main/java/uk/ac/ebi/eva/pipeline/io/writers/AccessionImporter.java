/*
 * Copyright 2023 EMBL - European Bioinformatics Institute
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

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;

import java.util.ArrayList;
import java.util.List;

import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.IDS_FIELD;

public class AccessionImporter extends MongoItemWriter<Variant> {
    private static final Logger logger = LoggerFactory.getLogger(AccessionImporter.class);
    private final MongoOperations mongoOperations;
    private final String collection;

    public AccessionImporter(String collection, MongoOperations mongoOperations) {
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        this.mongoOperations = mongoOperations;
        this.collection = collection;

        setTemplate(mongoOperations);
    }

    @Override
    protected void doWrite(List<? extends Variant> variants) {
        List<WriteModel<Document>> writes = new ArrayList<>();
        for (Variant variant : variants) {

            Assert.notNull(variant, "Variant should not be null. Please provide a valid Variant object");
            logger.debug("Convert variant {} into mongo object", variant);
            Assert.notNull(variant.getIds());
            Assert.notEmpty(variant.getIds());

            String id = VariantDocument.buildVariantId(variant.getChromosome(), variant.getStart(),
                    variant.getReference(), variant.getAlternate());

            // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
            Document query = new Document("_id", id)
                    .append(VariantDocument.CHROMOSOME_FIELD, variant.getChromosome())
                    .append(VariantDocument.START_FIELD, variant.getStart());

            writes.add(new UpdateOneModel<>(query, generateUpdate(variant), new UpdateOptions().upsert(false)));
        }

        if (!writes.isEmpty()) {
            logger.info("Execute bulk. BulkSize : " + writes.size());
            mongoOperations.getCollection(collection).bulkWrite(writes);
        }
    }

    private Document generateUpdate(Variant variant) {
        Document addToSet = new Document();
        addToSet.append(IDS_FIELD, new Document("$each", variant.getIds()));

        Document update = new Document();
        if (!addToSet.isEmpty()) {
            update.put("$addToSet", addToSet);
        }

        return update;
    }
}