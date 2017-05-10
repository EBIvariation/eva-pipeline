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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Score;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Xref;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.VEP_VERSION_FIELD;
import static uk.ac.ebi.eva.commons.models.data.VariantAnnotation.XREFS_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.documents.Annotation.VEP_CACHE_VERSION_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType.SO_ACCESSION_FIELD;

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
    private static final Logger logger = LoggerFactory.getLogger(AnnotationInVariantMongoWriter.class);

    private final MongoOperations mongoOperations;

    private final String collection;

    private String vepVersion;

    private String vepCacheVersion;

    private final String ANNOTATION_XREFS = VariantToDBObjectConverter.ANNOTATION_FIELD + ".$." + XREFS_FIELD;

    private final String ANNOTATION_SO = VariantToDBObjectConverter.ANNOTATION_FIELD + ".$." + SO_ACCESSION_FIELD;

    private final String ANNOTATION_SIFT = VariantToDBObjectConverter.ANNOTATION_FIELD + ".$." + SIFT_FIELD;

    private final String ANNOTATION_POLYPHEN = VariantToDBObjectConverter.ANNOTATION_FIELD + ".$." + POLYPHEN_FIELD;

    private final String ANNOTATION_ENSEMBL_VERSION = VariantToDBObjectConverter.ANNOTATION_FIELD + "." + VEP_VERSION_FIELD;

    private final String ANNOTATION_VEP_CACHE_VERSION = VariantToDBObjectConverter.ANNOTATION_FIELD + "." + VEP_CACHE_VERSION_FIELD;

    public AnnotationInVariantMongoWriter(MongoOperations mongoOperations,
                                          String collection,
                                          String vepVersion,
                                          String vepCacheVersion) {
        super();
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        this.mongoOperations = mongoOperations;
        this.collection = collection;
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }

    // TODO rewrite this to be bulk-friendly
    @Override
    public void write(List<? extends Annotation> annotations) throws Exception {
        Map<String, List<Annotation>> annotationsByStorageId = groupAnnotationByVariantId(annotations);

        for (Map.Entry<String, List<Annotation>> annotationsIdEntry : annotationsByStorageId.entrySet()) {
            VariantAnnotation variantAnnotation = extractFieldsFromAnnotations(annotationsIdEntry.getValue());

            String storageId = annotationsIdEntry.getKey();
            BasicDBObject id = new BasicDBObject("_id", storageId);

            DBObject dbObject = mongoOperations.getCollection(collection).findOne(id);

            if (dbObject != null) {
                logger.trace("Writing annotations into variant : {}, collection: {}", storageId, collection);

                List<BasicDBObject> existingAnnotations = (List<BasicDBObject>) dbObject
                        .get(VariantToDBObjectConverter.ANNOTATION_FIELD);

                if (existingAnnotations != null) {
                    updateExistingVariantAnnotation(variantAnnotation, storageId, existingAnnotations);
                } else {
                    addNewVariantAnnotation(variantAnnotation, id);
                }

            }
        }
    }

    /**
     * Update {@link VariantAnnotation} fields if some are already present in the {@link uk.ac.ebi.eva.commons.models.data.Variant}
     * Just make sure to update the specific version of annotation!
     *
     * @param variantAnnotation   the current annotation to append
     * @param storageId
     * @param existingAnnotations already in {@link uk.ac.ebi.eva.commons.models.data.Variant}
     */
    private void updateExistingVariantAnnotation(VariantAnnotation variantAnnotation,
                                                 String storageId,
                                                 List<BasicDBObject> existingAnnotations) {
        for (BasicDBObject existingAnnotation : existingAnnotations) {
            if (existingAnnotation.getString(VEP_VERSION_FIELD)
                    .equals(vepVersion) && existingAnnotation.getString(VEP_CACHE_VERSION_FIELD)
                    .equals(vepCacheVersion)) {

                Set<String> xrefs = variantAnnotation.getXrefIds();
                BasicDBObject addToSetValue = new BasicDBObject(ANNOTATION_XREFS, new BasicDBObject("$each", xrefs));

                Set<Integer> soAccessions = variantAnnotation.getSoAccessions();
                addToSetValue.append(ANNOTATION_SO, new BasicDBObject("$each", soAccessions));

                BasicDBObject update = new BasicDBObject("$addToSet", addToSetValue);

                variantAnnotation
                        .addSifts(lookupExistingSubstitutionScore((BasicDBList) existingAnnotation.get(SIFT_FIELD)));
                Set<Double> sifts = calculateRangeOfScores(variantAnnotation.getSifts());
                BasicDBObject setValue = new BasicDBObject(ANNOTATION_SIFT, sifts);

                variantAnnotation.addPolyphens(
                        lookupExistingSubstitutionScore((BasicDBList) existingAnnotation.get(POLYPHEN_FIELD)));
                Set<Double> polyphens = calculateRangeOfScores(variantAnnotation.getPolyphens());
                setValue.append(ANNOTATION_POLYPHEN, polyphens);

                update.append("$set", setValue);

                BasicDBObject versionedId = new BasicDBObject("_id", storageId);
                versionedId.append(ANNOTATION_ENSEMBL_VERSION, vepVersion);
                versionedId.append(ANNOTATION_VEP_CACHE_VERSION, vepCacheVersion);

                mongoOperations.updateFirst(new BasicQuery(versionedId), new BasicUpdate(update), collection);
            }
        }
    }

    /**
     * Append a new {@link VariantAnnotation} field into {@link uk.ac.ebi.eva.commons.models.data.Variant}
     *
     * @param variantAnnotation
     * @param id
     */
    private void addNewVariantAnnotation(VariantAnnotation variantAnnotation, BasicDBObject id) {
        Set<VariantAnnotation> variantAnnotations = new HashSet<>(Collections.singletonList(variantAnnotation));
        BasicDBObject updateAnnotation = new BasicDBObject("$set", new BasicDBObject(
                VariantToDBObjectConverter.ANNOTATION_FIELD, variantAnnotations));
        mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateAnnotation), collection);
    }

    private Set<Double> lookupExistingSubstitutionScore(BasicDBList scores) {
        Set<Double> substitutionScores = new HashSet<>();

        if (scores != null) {
            substitutionScores.addAll(scores.stream().map(score -> (Double) score).collect(Collectors.toSet()));
        }

        return substitutionScores;
    }

    /**
     * Extract Xrefs, so terms and protein substitution score from {@link Annotation}
     */
    private VariantAnnotation extractFieldsFromAnnotations(List<Annotation> annotations) {
        VariantAnnotation variantAnnotation = new VariantAnnotation(vepVersion, vepCacheVersion);

        for (Annotation annotation : annotations) {
            Set<Xref> xrefs = annotation.getXrefs();
            if (xrefs != null) {
                variantAnnotation.addXrefIds(xrefs.stream().map(Xref::getId).collect(Collectors.toSet()));
            }

            extractSubstitutionScores(variantAnnotation, annotation.getConsequenceTypes());
        }

        return variantAnnotation;
    }

    private void extractSubstitutionScores(VariantAnnotation variantAnnotation, Set<ConsequenceType> consequenceTypes) {
        if (consequenceTypes != null) {
            for (ConsequenceType consequenceType : consequenceTypes) {
                Score sift = consequenceType.getSift();
                if (sift != null) {
                    variantAnnotation.addSift(sift.getScore());
                }

                Score polyphen = consequenceType.getPolyphen();
                if (polyphen != null) {
                    variantAnnotation.addPolyphen(polyphen.getScore());
                }

                variantAnnotation.addsoAccessions(consequenceType.getSoAccessions());
            }
        }
    }

    /**
     * Return the min and max in case of multiple ProteinSubstitutionScores (sift/polyphen...)
     */
    private Set<Double> calculateRangeOfScores(Set<Double> scores) {
        if (scores.size() <= 1) {
            return scores;
        } else {
            return new HashSet<>(Arrays.asList(Collections.min(scores), Collections.max(scores)));
        }
    }

    private Map<String, List<Annotation>> groupAnnotationByVariantId(List<? extends Annotation> annotations) {
        Map<String, List<Annotation>> annotationsByVariantId = new HashMap<>();
        for (Annotation annotation : annotations) {
            String id = annotation.buildVariantId();
            annotationsByVariantId.putIfAbsent(id, new ArrayList<>());
            annotationsByVariantId.get(id).add(annotation);
        }

        return annotationsByVariantId;
    }

}
