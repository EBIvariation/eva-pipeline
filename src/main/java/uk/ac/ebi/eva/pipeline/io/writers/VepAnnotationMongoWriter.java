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
import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.ac.ebi.eva.commons.models.converters.data.AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.commons.models.converters.data.AnnotationFieldNames.XREFS_FIELD;

/**
 * Write a list of {@link VariantAnnotation} into MongoDB
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
public class VepAnnotationMongoWriter extends MongoItemWriter<VariantAnnotation> {
    private static final Logger logger = LoggerFactory.getLogger(VepAnnotationMongoWriter.class);

    private MongoOperations mongoOperations;

    private String collection;

    private String vepVersion;

    private String vepCacheVersion;

    public VepAnnotationMongoWriter(MongoOperations mongoOperations,
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
    }

    private Map<String, List<VariantAnnotation>> groupVariantAnnotationById(List<? extends VariantAnnotation> variantAnnotations) {
        Map<String, List<VariantAnnotation>> variantAnnotationsByStorageId = new HashMap<>();
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            String id = buildAnnotationtorageId(variantAnnotation);

            variantAnnotationsByStorageId.putIfAbsent(id, new ArrayList<>());
            variantAnnotationsByStorageId.get(id).add(variantAnnotation);
        }

        return variantAnnotationsByStorageId;
    }

    @Override
    protected void doWrite(List<? extends VariantAnnotation> variantAnnotations) {
        Map<String, List<VariantAnnotation>> variantAnnotationsByStorageId = groupVariantAnnotationById(
                variantAnnotations);

        for (Map.Entry<String, List<VariantAnnotation>> annotationsIdEntry : variantAnnotationsByStorageId.entrySet()) {
            String storageId = annotationsIdEntry.getKey();
            List<VariantAnnotation> annotations = annotationsIdEntry.getValue();

            VariantAnnotation variantAnnotation = annotations.get(0);

            if (annotations.size() > 1) {
                variantAnnotation = concatenateOtherAnnotations(
                        variantAnnotation, annotations.subList(1, annotations.size()));
            }

            variantAnnotation.setId(storageId);
            variantAnnotation.setEnsmblVersion(vepVersion);
            variantAnnotation.setVepCacheVersion(vepCacheVersion);

            variantAnnotation.extractXrefsFromConsequenceTypes();

            writeVariantAnnotationInMongoDb(storageId, variantAnnotation);
        }
    }

    /**
     * Append multiple annotation into a single {@link VariantAnnotation}
     * Updated fields are ConsequenceTypes and Hgvs
     *
     * @param variantAnnotation             annotation where other annotations will be appended
     * @param otherAnnotationsToConcatenate annotations to be appended
     * @return a single {@link VariantAnnotation} ready to be persisted
     */
    private VariantAnnotation concatenateOtherAnnotations(VariantAnnotation variantAnnotation,
                                                          List<VariantAnnotation> otherAnnotationsToConcatenate) {

        for (VariantAnnotation annotationToAppend : otherAnnotationsToConcatenate) {
            if (annotationToAppend.getConsequenceTypes() != null) {
                variantAnnotation.getConsequenceTypes().addAll(annotationToAppend.getConsequenceTypes());
            }
        }

        return variantAnnotation;
    }

    private void writeVariantAnnotationInMongoDb(String storageId, VariantAnnotation variantAnnotation) {
        logger.trace("Writing annotations into mongo id: {}", storageId);

        BasicDBObject id = new BasicDBObject("_id", storageId);

        if (mongoOperations.exists(new BasicQuery(id), collection)) {
            BasicDBObject updateConsequenceTypes = new BasicDBObject("$addToSet",
                    new BasicDBObject(CONSEQUENCE_TYPE_FIELD,
                            new BasicDBObject("$each",  variantAnnotation.getConsequenceTypes())));
            BasicDBObject updateXrefs = new BasicDBObject("$addToSet",
                    new BasicDBObject(XREFS_FIELD, new BasicDBObject("$each", variantAnnotation.getXrefs())));

            mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateConsequenceTypes), collection);
            mongoOperations.upsert(new BasicQuery(id), new BasicUpdate(updateXrefs), collection);
        } else {
            mongoOperations.save(variantAnnotation, collection);
        }
    }

    private String buildAnnotationtorageId(VariantAnnotation variantAnnotation) {
        return MongoDBHelper.buildAnnotationStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                                                      variantAnnotation.getReferenceAllele(),
                                                      variantAnnotation.getAlternativeAllele(), vepVersion,
                                                      vepCacheVersion);
    }

}
