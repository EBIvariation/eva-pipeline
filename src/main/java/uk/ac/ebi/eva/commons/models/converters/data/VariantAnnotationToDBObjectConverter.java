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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.annotation.Score;
import org.opencb.biodata.models.variant.annotation.Xref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Converts a {@link VariantAnnotation} into mongoDb {@link DBObject}
 * <p>
 * Slim version of {@link org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter}
 * Unused fields and methods removed.
 * <p>
 * The other way converter is {@link DBObjectToVariantAnnotationConverter}
 */
public class VariantAnnotationToDBObjectConverter implements Converter<VariantAnnotation, DBObject> {
    private static final Logger logger = LoggerFactory.getLogger(VariantAnnotationToDBObjectConverter.class);

    @Override
    public DBObject convert(VariantAnnotation variantAnnotation) {
        Assert.notNull(variantAnnotation,
                       "Variant should not be null. Please provide a valid VariantAnnotation object");
        logger.trace("Convert variant annotation into mongo object {} ", variantAnnotation);

        DBObject dbObject = new BasicDBObject();
        Set<DBObject> xrefs = new HashSet<>();
        List<DBObject> cts = new LinkedList<>();

        //ID
        if (variantAnnotation.getId() != null && !variantAnnotation.getId().isEmpty()) {
            xrefs.add(convertXrefToStorage(variantAnnotation.getId(), "dbSNP"));
        }

        //ConsequenceType
        if (variantAnnotation.getConsequenceTypes() != null) {
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            for (ConsequenceType consequenceType : consequenceTypes) {
                DBObject ct = new BasicDBObject();

                putNotNull(ct, AnnotationFieldNames.GENE_NAME_FIELD, consequenceType.getGeneName());
                putNotNull(ct, AnnotationFieldNames.ENSEMBL_GENE_ID_FIELD, consequenceType.getEnsemblGeneId());
                putNotNull(ct, AnnotationFieldNames.ENSEMBL_TRANSCRIPT_ID_FIELD, consequenceType.getEnsemblTranscriptId());
                putNotNull(ct, AnnotationFieldNames.RELATIVE_POS_FIELD, consequenceType.getRelativePosition());
                putNotNull(ct, AnnotationFieldNames.CODON_FIELD, consequenceType.getCodon());
                putNotNull(ct, AnnotationFieldNames.STRAND_FIELD, consequenceType.getStrand());
                putNotNull(ct, AnnotationFieldNames.BIOTYPE_FIELD, consequenceType.getBiotype());
                putNotNull(ct, AnnotationFieldNames.C_DNA_POSITION_FIELD, consequenceType.getcDnaPosition());
                putNotNull(ct, AnnotationFieldNames.CDS_POSITION_FIELD, consequenceType.getCdsPosition());
                putNotNull(ct, AnnotationFieldNames.AA_POSITION_FIELD, consequenceType.getAaPosition());
                putNotNull(ct, AnnotationFieldNames.AA_CHANGE_FIELD, consequenceType.getAaChange());

                if (consequenceType.getSoTerms() != null) {
                    List<Integer> soAccession = new LinkedList<>();
                    for (ConsequenceType.ConsequenceTypeEntry entry : consequenceType.getSoTerms()) {
                        soAccession.add(ConsequenceTypeMappings.termToAccession.get(entry.getSoName()));
                    }
                    putNotNull(ct, AnnotationFieldNames.SO_ACCESSION_FIELD, soAccession);
                }

                //Protein substitution region score
                if (consequenceType.getProteinSubstitutionScores() != null) {
                    List<DBObject> proteinSubstitutionScores = new LinkedList<>();
                    for (Score score : consequenceType.getProteinSubstitutionScores()) {
                        if (score != null) {
                            if (score.getSource().equals("Polyphen")) {
                                putNotNull(ct, AnnotationFieldNames.POLYPHEN_FIELD,
                                           convertScoreToStorage(score.getScore(), null, score.getDescription()));
                            } else if (score.getSource().equals("Sift")) {
                                putNotNull(ct, AnnotationFieldNames.SIFT_FIELD,
                                           convertScoreToStorage(score.getScore(), null, score.getDescription()));
                            } else {
                                proteinSubstitutionScores.add(convertScoreToStorage(score));
                            }
                        }
                    }
                    putNotNull(ct, AnnotationFieldNames.PROTEIN_SUBSTITUTION_SCORE_FIELD, proteinSubstitutionScores);
                }


                cts.add(ct);

                if (consequenceType.getGeneName() != null && !consequenceType.getGeneName().isEmpty()) {
                    xrefs.add(convertXrefToStorage(consequenceType.getGeneName(), "HGNC"));
                }
                if (consequenceType.getEnsemblGeneId() != null && !consequenceType.getEnsemblGeneId().isEmpty()) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblGeneId(), "ensemblGene"));
                }
                if (consequenceType.getEnsemblTranscriptId() != null && !consequenceType.getEnsemblTranscriptId()
                        .isEmpty()) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblTranscriptId(), "ensemblTranscript"));
                }

            }
            putNotNull(dbObject, AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD, cts);
        }

        //XREFs
        if (variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSrc()));
            }
        }
        putNotNull(dbObject, AnnotationFieldNames.XREFS_FIELD, xrefs);

        return dbObject;
    }

    private DBObject convertScoreToStorage(Score score) {
        return convertScoreToStorage(score.getScore(), score.getSource(), score.getDescription());
    }

    private DBObject convertScoreToStorage(double score, String source, String description) {
        DBObject dbObject = new BasicDBObject(AnnotationFieldNames.SCORE_SCORE_FIELD, score);
        putNotNull(dbObject, AnnotationFieldNames.SCORE_SOURCE_FIELD, source);
        putNotNull(dbObject, AnnotationFieldNames.SCORE_DESCRIPTION_FIELD, description);
        return dbObject;
    }

    private DBObject convertXrefToStorage(String id, String source) {
        DBObject dbObject = new BasicDBObject(AnnotationFieldNames.XREF_ID_FIELD, id);
        dbObject.put(AnnotationFieldNames.XREF_SOURCE_FIELD, source);
        return dbObject;
    }


    private void putNotNull(DBObject dbObject, String key, Object obj) {
        if (obj != null) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, Collection obj) {
        if (obj != null && !obj.isEmpty()) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, String obj) {
        if (obj != null && !obj.isEmpty()) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, Integer obj) {
        if (obj != null && obj != 0) {
            dbObject.put(key, obj);
        }
    }

}
