/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter.ANNOTATION_FIELD;
import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.VEP_VERSION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.documents.Annotation.VEP_CACHE_VERSION_FIELD;

/**
 * Update the {@link uk.ac.ebi.eva.commons.models.data.Variant} mongo document with {@link VariantAnnotation}
 * <p>
 * The fields updated are:
 * - sifts
 * - polyphens
 * - soAccessions
 * - Xref Ids
 */
public class AnnotationInVariantMongoWriter implements ItemWriter<Annotation> {

    public static final String ID = "_id";
    public static final String SET = "$set";
    public static final String ADD_TO_SET = "$addToSet";
    public static final String IN = "$in";
    public static final String ELEM_MATCH = "$elemMatch";
    public static final String ANNOTATION_IN_LIST = ANNOTATION_FIELD + ".$";

    private final MongoOperations mongoOperations;

    private final String collection;

    private final String vepVersion;

    private final String vepCacheVersion;

    public AnnotationInVariantMongoWriter(MongoOperations mongoOperations,
                                          String collection,
                                          String vepVersion,
                                          String vepCacheVersion) {
        super();
        Assert.notNull(mongoOperations);
        Assert.hasText(collection);
        Assert.hasText(vepVersion);
        Assert.hasText(vepCacheVersion);

        this.mongoOperations = mongoOperations;
        this.collection = collection;
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }
    
    private HashMap<String, VariantAnnotation> generateVariantAnnotations(List<? extends Annotation> annotations) {
        HashMap<String, VariantAnnotation> variantAnnotations = new HashMap<>();

        for (Annotation annotation : annotations) {
            String id = annotation.buildVariantId();
            variantAnnotations.putIfAbsent(id, new VariantAnnotation(annotation));
            variantAnnotations.computeIfPresent(id, (key, oldVar) -> oldVar.concatenate(annotation));
        }
        return variantAnnotations;
    }

    @Override
    public void write(List<? extends Annotation> annotations) throws Exception {
        Map<String, VariantAnnotation> variantAnnotations = generateVariantAnnotations(annotations);

        BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, collection);
        bulkPrepare(bulkOperations, variantAnnotations);
        bulkOperations.execute();
    }

    private void bulkPrepare(BulkOperations bulkOperations, Map<String, VariantAnnotation> variantAnnotations) {
        Map<String, VariantAnnotation> storedVariantAnnotations = getStoredVariantAnnotations(variantAnnotations);

        for (Map.Entry<String, VariantAnnotation> entry : variantAnnotations.entrySet()) {
            final String key = entry.getKey();
            if (storedVariantAnnotations.containsKey(key)) {
                bulkUpdate(bulkOperations, key, storedVariantAnnotations.get(key).concatenate(entry.getValue()));
            } else {
                bulkAddToSet(bulkOperations, key, entry.getValue());
            }
        }
    }

    private void bulkAddToSet(BulkOperations bulkOperations, String variantId, VariantAnnotation value) {
        DBObject id = new BasicDBObject(ID, variantId);
        DBObject variantAnnotation = convertToMongo(value);
        BasicDBObject addToSet = new BasicDBObject(ADD_TO_SET, new BasicDBObject(ANNOTATION_FIELD, variantAnnotation));
        bulkOperations.updateOne(new BasicQuery(id), new BasicUpdate(addToSet));
    }

    private void bulkUpdate(BulkOperations bulkOperations, String variantId, VariantAnnotation value) {
        BasicDBObject query = generateQueryForAnnotationInVariant(variantId);

        DBObject variantAnnotation = convertToMongo(value);
        final BasicDBObject annotation = new BasicDBObject(ANNOTATION_IN_LIST, variantAnnotation);
        BasicDBObject setAnnotation = new BasicDBObject(SET, annotation);

        bulkOperations.updateOne(new BasicQuery(query), new BasicUpdate(setAnnotation));
    }

    private BasicDBObject generateQueryForAnnotationInVariant(String... variantIds) {
        BasicDBObject query = new BasicDBObject();
        if (variantIds.length == 1) {
            query.append(ID, variantIds[0]);
        } else {
            query.append(ID, new BasicDBObject(IN, variantIds));
        }
        query.append(ANNOTATION_FIELD, createQueryMatchForVepAndCacheVersion());
        return query;
    }

    private BasicDBObject createQueryMatchForVepAndCacheVersion() {
        BasicDBObject annotationQuery = new BasicDBObject();
        annotationQuery.append(VEP_VERSION_FIELD, vepVersion);
        annotationQuery.append(VEP_CACHE_VERSION_FIELD, vepCacheVersion);
        return new BasicDBObject(ELEM_MATCH, annotationQuery);
    }

    private Map<String, VariantAnnotation> getStoredVariantAnnotations(Map<String, VariantAnnotation> variantAnnotations) {
        Map<String, VariantAnnotation> storedVariantAnnotations = new HashMap<>();
        BasicDBObject query = generateQueryForAnnotationInVariant(variantAnnotations.keySet().toArray(new String[]{}));

        Iterator<DBObject> iterator = mongoOperations.getCollection(collection).find(query).iterator();
        while (iterator.hasNext()) {
            final DBObject object = iterator.next();
            final String id = (String) object.get(ID);
            final BasicDBList dbAnnotations = (BasicDBList) object.get(ANNOTATION_FIELD);
            if (dbAnnotations != null && !dbAnnotations.isEmpty()) {
                final DBObject dbAnnotation = (DBObject) dbAnnotations.get(0);
                storedVariantAnnotations.put(id, convertToVariantAnnotation(dbAnnotation));
            }
        }
        return storedVariantAnnotations;
    }

    private VariantAnnotation convertToVariantAnnotation(DBObject dbAnnotation) {
        return mongoOperations.getConverter().read(VariantAnnotation.class, dbAnnotation);
    }

    private DBObject convertToMongo(VariantAnnotation value) {
        return (DBObject) mongoOperations.getConverter().convertToMongoType(value);
    }
}
