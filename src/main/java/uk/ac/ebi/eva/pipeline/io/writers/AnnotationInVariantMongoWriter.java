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
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.ALTERNATE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.ANNOTATION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.CHROMOSOME_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.REFERENCE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.START_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.VEP_CACHE_VERSION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation.VEP_VERSION_FIELD;

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
        Assert.notNull(mongoOperations);
        Assert.hasText(collection);
        Assert.hasText(vepVersion);
        Assert.hasText(vepCacheVersion);

        this.mongoOperations = mongoOperations;
        this.collection = collection;
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }

    @Override
    public void write(List<? extends Annotation> annotations) throws Exception {
        Map<String, VariantAnnotation> variantAnnotations = generateVariantAnnotations(annotations);

        BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, collection);
        bulkPrepare(bulkOperations, variantAnnotations);
        bulkOperations.execute();
    }

    private Map<String, VariantAnnotation> generateVariantAnnotations(List<? extends Annotation> annotations) {
        HashMap<String, VariantAnnotation> variantAnnotations = new HashMap<>();

        for (Annotation annotation : annotations) {
            String id = annotation.getId();
            variantAnnotations.putIfAbsent(id, new VariantAnnotation(annotation));
            variantAnnotations.computeIfPresent(id, (key, oldVar) -> oldVar.concatenate(annotation));
        }
        return variantAnnotations;
    }

    private void bulkPrepare(BulkOperations bulkOperations, Map<String, VariantAnnotation> variantAnnotations) {
        Map<String, VariantAnnotation> storedVariantAnnotations = getStoredVariantAnnotations(variantAnnotations);

        for (Map.Entry<String, VariantAnnotation> entry : variantAnnotations.entrySet()) {
            final String annotationId = entry.getKey();
            if (storedVariantAnnotations.containsKey(annotationId)) {
                bulkUpdate(bulkOperations, annotationId,
                           storedVariantAnnotations.get(annotationId).concatenate(entry.getValue()));
            } else {
                bulkAddToSet(bulkOperations, annotationId, entry.getValue());
            }
        }
    }

    private Map<String, VariantAnnotation> getStoredVariantAnnotations(
            Map<String, VariantAnnotation> variantAnnotations) {
        Map<String, VariantAnnotation> storedVariantAnnotations = new HashMap<>();
        BasicDBObject query = generateQueryForAnnotationInVariant(variantAnnotations.keySet().toArray(new String[]{}));
        BasicDBObject projection = new BasicDBObject(ANNOTATION_FIELD, 1);
        for (DBObject variantDocument : mongoOperations.getCollection(collection).find(query, projection)) {
            final BasicDBList dbAnnotations = (BasicDBList) variantDocument.get(ANNOTATION_FIELD);
            if (dbAnnotations != null && !dbAnnotations.isEmpty()) {
                for (Object storedAnnotationDocument : dbAnnotations) {
                    VariantAnnotation storedAnnotation = convertToVariantAnnotation(
                            (DBObject) storedAnnotationDocument);
                    final String annotationId = getAnnotationId(variantDocument, storedAnnotation);
                    storedVariantAnnotations.put(annotationId, storedAnnotation);
                }
            }
        }
        return storedVariantAnnotations;
    }

    private String getAnnotationId(DBObject object, VariantAnnotation storedAnnotation) {
        return String.join("_", (String) object.get(ID),
                           storedAnnotation.getVepVersion(),
                           storedAnnotation.getVepCacheVersion());
    }

    private BasicDBObject generateQueryForAnnotationInVariant(String... annotationIds) {
        BasicDBObject query = new BasicDBObject();
        if (annotationIds.length == 1) {
            query.append(ID, getVariantId(annotationIds[0]));
        } else {
            List<String> ids = Arrays.stream(annotationIds).map(this::getVariantId).collect(toList());
            query.append(ID, new BasicDBObject(IN, ids));
        }
        query.append(ANNOTATION_FIELD, createQueryMatchForVepAndCacheVersion());
        return query;
    }

    private String getVariantId(String annotationId) {
        return annotationId.substring(0, annotationId.length() - vepVersion.length() - vepCacheVersion.length() - 2);
    }

    private BasicDBObject createQueryMatchForVepAndCacheVersion() {
        BasicDBObject annotationQuery = new BasicDBObject();
        annotationQuery.append(VEP_VERSION_FIELD, vepVersion);
        annotationQuery.append(VEP_CACHE_VERSION_FIELD, vepCacheVersion);
        return new BasicDBObject(ELEM_MATCH, annotationQuery);
    }

    private VariantAnnotation convertToVariantAnnotation(DBObject dbAnnotation) {
        return mongoOperations.getConverter().read(VariantAnnotation.class, dbAnnotation);
    }

    private void bulkUpdate(BulkOperations bulkOperations, String annotationId, VariantAnnotation value) {
        BasicDBObject query = generateQueryForAnnotationInVariant(annotationId);

        DBObject variantAnnotation = convertToMongo(value);
        final BasicDBObject annotation = new BasicDBObject(ANNOTATION_IN_LIST, variantAnnotation);
        BasicDBObject setAnnotation = new BasicDBObject(SET, annotation);

        bulkOperations.updateOne(new BasicQuery(query), new BasicUpdate(setAnnotation));
    }

    private void bulkAddToSet(BulkOperations bulkOperations, String annotationId, VariantAnnotation value) {
        DBObject id = new BasicDBObject(ID, getVariantId(annotationId));
        DBObject variantAnnotation = convertToMongo(value);
        BasicDBObject addToSet = new BasicDBObject(ADD_TO_SET, new BasicDBObject(ANNOTATION_FIELD, variantAnnotation));
        bulkOperations.updateOne(new BasicQuery(id), new BasicUpdate(addToSet));
    }

    private DBObject convertToMongo(VariantAnnotation value) {
        return (DBObject) mongoOperations.getConverter().convertToMongoType(value);
    }
}
