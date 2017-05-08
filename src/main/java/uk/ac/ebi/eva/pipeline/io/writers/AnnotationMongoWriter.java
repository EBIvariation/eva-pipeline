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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames.XREFS_FIELD;

/**
 * Write a list of {@link Annotation} into MongoDB
 * <p>
 * A new annotation is added in the existing document.
 * In case of two annotations (or more) in the same variant the other annotations are appended:
 * <p>
 * 20_63963_G/A    20:63963   A  ENSG00000178591    ENST00000382410    Transcript upstream_gene_variant  -  -  -  -  -  -  DISTANCE=4388;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE
 * 20_63963_G/A    20:63963   A  ENSG00000178591    ENST00000608838    Transcript upstream_gene_variant  -  -  -  -  -  -  DISTANCE=3928;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript
 * <p>
 * Will be:
 * <p>
 * "annot" : {
 * "ct" : [
 * { "gn" : "DEFB125", "ensg" : "ENSG00000178591", "enst" : "ENST00000382410", "codon" : "-", "strand" : "+", "bt" : "protein_coding", "aaChange" : "-", "so" : [ 1631 ] },
 * { "gn" : "DEFB125", "ensg" : "ENSG00000178591", "enst" : "ENST00000608838", "codon" : "-", "strand" : "+", "bt" : "processed_transcript", "aaChange" : "-",
 * "so" : [ 1631 ] } ],
 * "xrefs" : [
 * { "id" : "DEFB125", "src" : "HGNC" },
 * { "id" : "ENST00000382410", "src" : "ensemblTranscript" },
 * { "id" : "ENST00000608838", "src" : "ensemblTranscript" },
 * { "id" : "ENSG00000178591", "src" : "ensemblGene"
 */
public class AnnotationMongoWriter extends MongoItemWriter<Annotation> {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationMongoWriter.class);

    private static final String ANNOTATION_XREF_ID_FIELD = "xrefs.id";

    private static final String ANNOTATION_CT_SO_FIELD = "ct.so";

    private MongoOperations mongoOperations;

    private String collection;

    private String vepVersion;

    private String vepCacheVersion;

    public AnnotationMongoWriter(MongoOperations mongoOperations,
                                 String collection,
                                 String vepVersion,
                                 String vepCacheVersion) {
        super();
        Assert.notNull(mongoOperations, "A Mongo instance is required");
        Assert.hasText(collection, "A collection name is required");

        setCollection(collection);
        setTemplate(mongoOperations);

        this.mongoOperations = mongoOperations;
        this.collection = collection;
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;

        createIndexes();
    }

    @Override
    protected void doWrite(List<? extends Annotation> annotations) {
        Map<String, List<Annotation>> annotationsByStorageId = groupAnnotationById(annotations);

        for (Map.Entry<String, List<Annotation>> annotationsIdEntry : annotationsByStorageId.entrySet()) {
            String storageId = annotationsIdEntry.getKey();
            List<Annotation> annotationsById = annotationsIdEntry.getValue();

            Annotation annotation = annotationsById.get(0);

            if (annotationsById.size() > 1) {
                annotation = concatenateOtherAnnotations(
                        annotation, annotationsById.subList(1, annotationsById.size()));
            }

            annotation.setId(storageId);
            annotation.setEnsemblVersion(vepVersion);
            annotation.setVepCacheVersion(vepCacheVersion);

            annotation.generateXrefsFromConsequenceTypes();

            writeAnnotationInMongoDb(storageId, annotation);
        }
    }

    private Map<String, List<Annotation>> groupAnnotationById(List<? extends Annotation> annotations) {
        Map<String, List<Annotation>> annotationsByStorageId = new HashMap<>();
        for (Annotation annotation : annotations) {
            String id = buildAnnotationtorageId(annotation);

            annotationsByStorageId.putIfAbsent(id, new ArrayList<>());
            annotationsByStorageId.get(id).add(annotation);
        }

        return annotationsByStorageId;
    }

    /**
     * Append multiple annotation into a single {@link Annotation}
     * Updated fields are ConsequenceTypes and Hgvs
     *
     * @param annotation                    annotation where other annotations will be appended
     * @param otherAnnotationsToConcatenate annotations to be appended
     * @return a single {@link Annotation} ready to be persisted
     */
    private Annotation concatenateOtherAnnotations(Annotation annotation,
                                                   List<Annotation> otherAnnotationsToConcatenate) {

        for (Annotation annotationToAppend : otherAnnotationsToConcatenate) {
            if (annotationToAppend.getConsequenceTypes() != null) {
                annotation.getConsequenceTypes().addAll(annotationToAppend.getConsequenceTypes());
            }
        }

        return annotation;
    }

    private void writeAnnotationInMongoDb(String storageId, Annotation annotation) {
        logger.trace("Writing annotations into mongo id: {}", storageId);

        BasicDBObject id = new BasicDBObject("_id", storageId);

        if (mongoOperations.exists(new BasicQuery(id), collection)) {
            BasicDBObject updateConsequenceTypes = new BasicDBObject("$addToSet",
                    new BasicDBObject(CONSEQUENCE_TYPE_FIELD,
                            new BasicDBObject("$each",annotation.getConsequenceTypes())));
            BasicDBObject updateXrefs = new BasicDBObject("$addToSet",
                    new BasicDBObject(XREFS_FIELD, new BasicDBObject("$each", annotation.getXrefs())));

            mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateConsequenceTypes), collection);
            mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateXrefs), collection);
        } else {
            mongoOperations.save(annotation, collection);
        }
    }

    private String buildAnnotationtorageId(Annotation annotation) {
        return MongoDBHelper.buildAnnotationStorageId(annotation.getChromosome(), annotation.getStart(),
                                                      annotation.getReferenceAllele(),
                                                      annotation.getAlternativeAllele(), vepVersion,
                                                      vepCacheVersion);
    }

    private void createIndexes() {
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(ANNOTATION_XREF_ID_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
        mongoOperations.getCollection(collection).createIndex(
                new BasicDBObject(ANNOTATION_CT_SO_FIELD, 1),
                new BasicDBObject(MongoDBHelper.BACKGROUND_INDEX, true));
    }
}
