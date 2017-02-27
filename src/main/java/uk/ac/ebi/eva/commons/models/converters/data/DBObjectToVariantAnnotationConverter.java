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

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.annotation.Score;
import org.opencb.biodata.models.variant.annotation.Xref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;

import java.util.LinkedList;
import java.util.List;

/**
 * Converts a mongoDb {@link DBObject} into {@link VariantAnnotation}
 * <p>
 * Slim version of {@link org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter}
 * Unused fields and methods removed.
 * <p>
 * The other way converter is {@link VariantAnnotationToDBObjectConverter}
 */
public class DBObjectToVariantAnnotationConverter implements Converter<DBObject, VariantAnnotation> {
    private static final Logger logger = LoggerFactory.getLogger(DBObjectToVariantAnnotationConverter.class);

    @Override
    public VariantAnnotation convert(DBObject object) {
        logger.trace("Convert mongo object into variant annotation {} ", object);

        VariantAnnotation variantAnnotation = new VariantAnnotation();

        //ConsequenceType
        List<ConsequenceType> consequenceTypes = new LinkedList<>();
        Object cts = object.get(AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD);
        if (cts != null && cts instanceof BasicDBList) {
            for (Object o : ((BasicDBList) cts)) {
                if (o instanceof DBObject) {
                    DBObject ct = (DBObject) o;

                    //SO accession name
                    List<String> soAccessionNames = new LinkedList<>();
                    if (ct.containsField(AnnotationFieldNames.SO_ACCESSION_FIELD)) {
                        if (ct.get(AnnotationFieldNames.SO_ACCESSION_FIELD) instanceof List) {
                            List<Integer> list = (List) ct.get(AnnotationFieldNames.SO_ACCESSION_FIELD);
                            for (Integer so : list) {
                                soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(so));
                            }
                        } else {
                            soAccessionNames
                                    .add(ConsequenceTypeMappings.accessionToTerm.get(ct.get(
                                            AnnotationFieldNames.SO_ACCESSION_FIELD)));
                        }
                    }

                    //ProteinSubstitutionScores
                    List<Score> proteinSubstitutionScores = new LinkedList<>();
                    if (ct.containsField(AnnotationFieldNames.PROTEIN_SUBSTITUTION_SCORE_FIELD)) {
                        List<DBObject> list = (List) ct.get(AnnotationFieldNames.PROTEIN_SUBSTITUTION_SCORE_FIELD);
                        for (DBObject dbObject : list) {
                            proteinSubstitutionScores.add(new Score(
                                    getDefault(dbObject, AnnotationFieldNames.SCORE_SCORE_FIELD, 0.0),
                                    getDefault(dbObject, AnnotationFieldNames.SCORE_SOURCE_FIELD, ""),
                                    getDefault(dbObject, AnnotationFieldNames.SCORE_DESCRIPTION_FIELD, "")
                            ));
                        }
                    }

                    if (ct.containsField(AnnotationFieldNames.POLYPHEN_FIELD)) {
                        DBObject dbObject = (DBObject) ct.get(AnnotationFieldNames.POLYPHEN_FIELD);
                        proteinSubstitutionScores.add(new Score(getDefault(dbObject, AnnotationFieldNames.SCORE_SCORE_FIELD, 0.0),
                                                                "Polyphen",
                                                                getDefault(dbObject, AnnotationFieldNames.SCORE_DESCRIPTION_FIELD, "")));
                    }

                    if (ct.containsField(AnnotationFieldNames.SIFT_FIELD)) {
                        DBObject dbObject = (DBObject) ct.get(AnnotationFieldNames.SIFT_FIELD);
                        proteinSubstitutionScores.add(new Score(getDefault(dbObject, AnnotationFieldNames.SCORE_SCORE_FIELD, 0.0),
                                                                "Sift",
                                                                getDefault(dbObject, AnnotationFieldNames.SCORE_DESCRIPTION_FIELD, "")));
                    }

                    consequenceTypes.add(new ConsequenceType(
                            getDefault(ct, AnnotationFieldNames.GENE_NAME_FIELD, "") /*.toString()*/,
                            getDefault(ct, AnnotationFieldNames.ENSEMBL_GENE_ID_FIELD, "") /*.toString()*/,
                            getDefault(ct, AnnotationFieldNames.ENSEMBL_TRANSCRIPT_ID_FIELD, "") /*.toString()*/,
                            getDefault(ct, AnnotationFieldNames.STRAND_FIELD, "") /*.toString()*/,
                            getDefault(ct, AnnotationFieldNames.BIOTYPE_FIELD, "") /*.toString()*/,
                            getDefault(ct, AnnotationFieldNames.C_DNA_POSITION_FIELD, 0),
                            getDefault(ct, AnnotationFieldNames.CDS_POSITION_FIELD, 0),
                            getDefault(ct, AnnotationFieldNames.AA_POSITION_FIELD, 0),
                            getDefault(ct, AnnotationFieldNames.AA_CHANGE_FIELD, "") /*.toString() */,
                            getDefault(ct, AnnotationFieldNames.CODON_FIELD, "") /*.toString() */,
                            proteinSubstitutionScores,
                            soAccessionNames));
                }
            }

        }
        variantAnnotation.setConsequenceTypes(consequenceTypes);

        //XREfs
        List<Xref> xrefs = new LinkedList<>();
        Object xrs = object.get(AnnotationFieldNames.XREFS_FIELD);
        if (xrs != null && xrs instanceof BasicDBList) {
            for (Object o : (BasicDBList) xrs) {
                if (o instanceof DBObject) {
                    DBObject xref = (DBObject) o;

                    xrefs.add(new Xref(
                            (String) xref.get(AnnotationFieldNames.XREF_ID_FIELD),
                            (String) xref.get(AnnotationFieldNames.XREF_SOURCE_FIELD))
                    );
                }
            }
        }
        variantAnnotation.setXrefs(xrefs);

        return variantAnnotation;
    }

    private String getDefault(DBObject object, String key, String defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            return o.toString();
        } else {
            return defaultValue;
        }
    }

    private int getDefault(DBObject object, String key, int defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Integer) {
                return (Integer) o;
            } else {
                try {
                    return Integer.parseInt(o.toString());
                } catch (Exception e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

    private double getDefault(DBObject object, String key, double defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Double) {
                return (Double) o;
            } else {
                try {
                    return Double.parseDouble(o.toString());
                } catch (Exception e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }
}
