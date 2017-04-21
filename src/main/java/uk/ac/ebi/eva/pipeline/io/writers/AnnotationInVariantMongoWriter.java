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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.data.Annotation;
import uk.ac.ebi.eva.commons.models.data.ConsequenceType;
import uk.ac.ebi.eva.commons.models.data.Score;
import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.commons.models.data.Xref;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames.GENE_NAME_FIELD;
import static uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames.POLYPHEN_FIELD;
import static uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames.SIFT_FIELD;
import static uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames.SO_ACCESSION_FIELD;

/**
 * Update the {@link uk.ac.ebi.eva.commons.models.data.Variant} mongo document with some fields from {@link Annotation}
 * and {@link ConsequenceType}
 * <p>
 * The fields are:
 * - sifts
 * - polyphens
 * - soAccessions
 * - Xref Ids
 */
public class AnnotationInVariantMongoWriter extends MongoItemWriter<Annotation> {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationInVariantMongoWriter.class);

    private final MongoOperations mongoOperations;

    private final String collection;

    public AnnotationInVariantMongoWriter(MongoOperations mongoOperations, String collection) {
        super();
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        setCollection(collection);
        setTemplate(mongoOperations);

        this.mongoOperations = mongoOperations;
        this.collection = collection;
    }

    @Override
    protected void doWrite(List<? extends Annotation> annotations) {
        Map<String, List<Annotation>> annotationsByStorageId = groupAnnotationById(annotations);

        for (Map.Entry<String, List<Annotation>> annotationsIdEntry : annotationsByStorageId.entrySet()) {
            VariantAnnotation variantAnnotation = extractFieldsFromAnnotations(annotationsIdEntry.getValue());

            String storageId = annotationsIdEntry.getKey();
            BasicDBObject id = new BasicDBObject("_id", storageId);

            if (mongoOperations.exists(new BasicQuery(id), collection)) {
                logger.trace("Writing annotations fields into mongo id: {}, collection: {}", storageId, collection);

                Set<String> xrefs = variantAnnotation.getXrefIds();
                BasicDBObject updateGeneNames = new BasicDBObject("$addToSet", new BasicDBObject(GENE_NAME_FIELD,
                                                                                                 new BasicDBObject(
                                                                                                         "$each",
                                                                                                         xrefs)));
                mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateGeneNames), collection);

                Set<Integer> soAccessions = variantAnnotation.getSoAccessions();
                BasicDBObject updateConsequenceTypes = new BasicDBObject("$addToSet",
                                                                         new BasicDBObject(SO_ACCESSION_FIELD,
                                                                                           new BasicDBObject("$each",
                                                                                                             soAccessions)));
                mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateConsequenceTypes), collection);

                variantAnnotation.addSifts(lookupExistingSubstitutionScore(SIFT_FIELD, storageId));
                Set<Double> sifts = calculateRangeOfScores(variantAnnotation.getSifts());
                updateSubstitutionScore(sifts, SIFT_FIELD, id);

                variantAnnotation.addPolyphens(lookupExistingSubstitutionScore(POLYPHEN_FIELD, storageId));
                Set<Double> polyphens = calculateRangeOfScores(variantAnnotation.getPolyphens());
                updateSubstitutionScore(polyphens, POLYPHEN_FIELD, id);

            } else {
                logger.info("Unable to update annotation fields into variant {} because it doesn't exist", storageId);
            }

        }
    }

    private void updateSubstitutionScore(Set<Double> substitutionScores,
                                         String substitutionScoreName,
                                         BasicDBObject id) {
        if (!substitutionScores.isEmpty()) {
            BasicDBObject updateSubstitutionScore = new BasicDBObject("$set", new BasicDBObject(substitutionScoreName,
                                                                                                substitutionScores));
            mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateSubstitutionScore), collection);
        }
    }

    /**
     * Checks for a given variant if a protein substitution score is already present
     *
     * @param substitutionScoreField substitution score name like POLYPHEN_FIELD, SIFT_FIELD etc.
     * @param storageId              variant ID
     * @return a set containing all the substitution scores if any already loaded, or an empty set otherwise
     */
    private Set<Double> lookupExistingSubstitutionScore(String substitutionScoreField, String storageId) {
        Set<Double> substitutionScores = new HashSet<>();

        BasicDBObject field = new BasicDBObject(substitutionScoreField, new BasicDBObject("$exists", true))
                .append("_id", storageId);

        BasicQuery fieldQuery = new BasicQuery(field);
        fieldQuery.fields().include(substitutionScoreField);
        fieldQuery.fields().exclude("_id");

        BasicDBObject existingFields = mongoOperations.findOne(fieldQuery, BasicDBObject.class, collection);

        if (existingFields != null) {
            BasicDBList scores = (BasicDBList) existingFields.getOrDefault(substitutionScoreField, new BasicDBList());
            substitutionScores.addAll(scores.stream().map(score -> (Double) score).collect(Collectors.toSet()));
        }

        return substitutionScores;
    }


    /**
     * Extract Xrefs, so terms and protein substitution score from {@link Annotation}
     */
    private VariantAnnotation extractFieldsFromAnnotations(List<Annotation> annotations) {
        VariantAnnotation variantAnnotation = new VariantAnnotation();

        for (Annotation annotation : annotations) {
            annotation.generateXrefsFromConsequenceTypes();
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

    private Map<String, List<Annotation>> groupAnnotationById(List<? extends Annotation> annotations) {
        Map<String, List<Annotation>> annotationsByStorageId = new HashMap<>();
        for (Annotation annotation : annotations) {
            String id = MongoDBHelper.buildVariantStorageId(annotation);

            annotationsByStorageId.putIfAbsent(id, new ArrayList<>());
            annotationsByStorageId.get(id).add(annotation);
        }

        return annotationsByStorageId;
    }

}
