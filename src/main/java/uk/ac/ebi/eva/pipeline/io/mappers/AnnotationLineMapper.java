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

package uk.ac.ebi.eva.pipeline.io.mappers;

import org.apache.commons.lang.ArrayUtils;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.LineMapper;

import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Score;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Map a line in VEP output file to {@link Annotation}
 *
 * Example of VEP output line
 * 20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60479_C/T	20:60479	T	-	-	-	intergenic_variant	-	-	-	-	-	rs149529999	GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 * 20_60523_-/C	20:60522-60523	C	-	-	-	intergenic_variant	-	-	-	-	-	rs150241001	GMAF=C:0.0115;AFR_MAF=C:0.05;AMR_MAF=C:0.0028
 *
 * Please note that most of the code is from org.opencb.biodata.formats.annotation.io.VepFormatReader
 * public methods in VepFormatReader can't be reused because there is a reference to the previous line (currentVariantString)
 * that prevent each line to be independent
 *
 * Here each line is mapped to {@link Annotation}; in case of two annotations for the same variant, a new
 * {@link Annotation} object is created containing only the fields that will be appended:
 *  - ConsequenceTypes
 *  - Hgvs
 */
public class AnnotationLineMapper implements LineMapper<Annotation> {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationLineMapper.class);

    private final String vepVersion;
    private final String vepCacheVersion;

    public AnnotationLineMapper(String vepVersion, String vepCacheVersion) {
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }

    /**
     * Map a line in VEP output file to {@link Annotation}
     * @param line in VEP output
     * @param lineNumber
     * @return a {@link Annotation}
     *
     * Most of the code is from org.opencb.biodata.formats.annotation.io.VepFormatReader#read() with few differences:
     *  - An empty array is initialized for Hgvs (like ConsequenceTypes);
     *  - parseFrequencies is always true and the all line is always parsed;
     *  - The logic to move around the file (read line) and reference to previous line (currentVariantString) are removed;
     */
    @Override
    public Annotation mapLine(String line, int lineNumber) {
        //logger.debug("Mapping line {} to Annotation", line);
        ConsequenceType consequenceType = new ConsequenceType();
        String[] lineFields = line.split("\t");

        Map<String,String> variantMap = parseVariant(lineFields[0], lineFields[1]);  // coordinates and alternative are only parsed once
        Annotation currentAnnotation = new Annotation(
                variantMap.get("chromosome"),
                Integer.valueOf(variantMap.get("start")),
                Integer.valueOf(variantMap.get("end")), variantMap.get("reference"),
                variantMap.get("alternative"),
                vepVersion,
                vepCacheVersion);

        /**
         * parses extra column and populates fields as required.
         * Some lines do not have extra field and end with a \t: the split function above does not return that field
         */
        if(lineFields.length == 14) {
            parseExtraField(consequenceType, lineFields[13]);
        }

        // Remaining fields only of interest if the feature is a transcript
        if(lineFields[5].toLowerCase().equals("transcript")) {
            parseTranscriptFields(consequenceType, lineFields);
            // Otherwise just set SO terms
        } else {
            consequenceType.setSoAccessions(mapSoTermsToSoAccessions(lineFields[6].split(",")));
        }
        currentAnnotation.addConsequenceType(consequenceType);
        
        return currentAnnotation;
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parseRemainingFields(org.opencb.biodata.models.variant.annotation.ConsequenceType, java.lang.String[])
     */
    private void parseTranscriptFields(ConsequenceType consequenceType, String[] lineFields) {
        consequenceType.setEnsemblGeneId(lineFields[3]);
        consequenceType.setEnsemblTranscriptId(lineFields[4]);
        if(!lineFields[6].equals("") && !lineFields[6].equals("-")) {  // VEP may leave this field empty
            consequenceType.setSoAccessions(mapSoTermsToSoAccessions(lineFields[6].split(",")));
        }
        if(!lineFields[7].equals("-")) {
            consequenceType.setcDnaPosition(parseStringInterval(lineFields[7]));
        }
        if(!lineFields[8].equals("-")) {
            consequenceType.setCdsPosition(parseStringInterval(lineFields[8]));
        }
        if(!lineFields[9].equals("-")) {
            consequenceType.setAaPosition(parseStringInterval(lineFields[9]));
        }
        consequenceType.setAaChange(lineFields[10]);
        consequenceType.setCodon(lineFields[11]);
    }

    private Set<Integer> mapSoTermsToSoAccessions(String[] soTerms){
        return Arrays.stream(soTerms).map(ConsequenceTypeMappings.termToAccession::get).collect(Collectors.toSet());
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader#parseStringInterval(java.lang.String)
     */
    private Integer parseStringInterval(String stringInterval) {
        String[] parts = stringInterval.split("-");
        if(!parts[0].equals("?")) {
            return Integer.valueOf(parts[0]);
        } else if(parts.length>1 && !parts[1].equals("?"))  {
            return Integer.valueOf(parts[1]);
        } else {
            return null;
        }
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader#parseVariant(java.lang.String, java.lang.String)
     */
    private Map<String,String> parseVariant(String variantString, String coordinatesString) {
//    private Map<String,String> parseVariant(String coordinatesString, String alternativeString) {

        Map<String, String> parsedVariant = new HashMap<>(5);

        try {
            String[] variantLocationFields = coordinatesString.split("[:-]");
//            parsedVariant.put("chromosome", variantLocationFields[0]);
//            parsedVariant.put("start", variantLocationFields[1]);
            parsedVariant.put("end", (variantLocationFields.length > 2) ? variantLocationFields[2] : variantLocationFields[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error("Unexpected format for column 2: "+coordinatesString);
            throw e;
        }

        try {
            // Some VEP examples:
            // 1_718787_-/T    1:718786-718787 T    ...
            // 1_718787_T/-    1:718787        -    ...
            // 1_718788_T/A    1:718788        A    ...
            String[] variantFields = variantString.split("[\\/]");
            //        String[] variantFields = variantString.split("[\\_\\/]");
            String[] leftVariantFields = variantFields[0].split("_");

            // Chr id containing _
            if(leftVariantFields.length>3) {
                parsedVariant.put("chromosome",
                        String.join("_", (String[]) ArrayUtils.subarray(leftVariantFields, 0, leftVariantFields.length - 2)));
            } else {
                parsedVariant.put("chromosome", leftVariantFields[0]);
            }
            parsedVariant.put("start", leftVariantFields[leftVariantFields.length-2]);
            parsedVariant.put("reference", leftVariantFields[leftVariantFields.length-1]);
            parsedVariant.put("alternative", variantFields[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error("Unexpected variant format for column 1: "+variantString);
            throw e;
        }

        return parsedVariant;
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parseExtraField(org.opencb.biodata.models.variant.annotation.ConsequenceType, java.lang.String, java.lang.Boolean)
     *
     * The parseFrequencies option has been removed
     */
    private void parseExtraField(ConsequenceType consequenceType, String extraField) {
        for (String field : extraField.split(";")) {
            String[] keyValue = field.split("=");

            switch (keyValue[0].toLowerCase()) {
                case "biotype":
                    consequenceType.setBiotype(keyValue[1]);
                    break;
                case "polyphen": // Format is PolyPhen=possibly_damaging(0.859)
                    consequenceType.setPolyphen(parseProteinSubstitutionScore(keyValue[1]));
                    break;
                case "sift": // Format is SIFT=tolerated(0.07)
                    consequenceType.setSift(parseProteinSubstitutionScore(keyValue[1]));
                    break;
                case "strand":
                    consequenceType.setStrand(keyValue[1].equals("1")?"+":"-");
                    break;
                case "symbol":
                    consequenceType.setGeneName(keyValue[1]);
                    break;
                default:
                    // ALLELE_NUM, FREQS, IND, ZYG
                    break;
            }
        }
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parseProteinSubstitutionScore(java.lang.String, java.lang.String)
     */
    private Score parseProteinSubstitutionScore(String scoreString) {
        String[] scoreFields = scoreString.split("[\\(\\)]");
        return new Score(Double.valueOf(scoreFields[1]), scoreFields[0]);
    }
}
