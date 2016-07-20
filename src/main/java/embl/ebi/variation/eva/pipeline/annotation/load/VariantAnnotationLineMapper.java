/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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

package embl.ebi.variation.eva.pipeline.annotation.load;

import org.apache.commons.lang.ArrayUtils;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.Score;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variation.PopulationFrequency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.LineMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Diego Poggioli
 *
 * Map a line in VEP output file to {@link VariantAnnotation}
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
 * Here each line is mapped to {@link VariantAnnotation}; in case of two annotations for the same variant, a new
 * {@link VariantAnnotation} object is created containing only the fields that will be appended:
 *  - ConsequenceTypes
 *  - Hgvs
 */
public class VariantAnnotationLineMapper implements LineMapper<VariantAnnotation> {
    private static final Logger logger = LoggerFactory.getLogger(VariantAnnotationLineMapper.class);

    /**
     * Map a line in VEP output file to {@link VariantAnnotation}
     * @param line in VEP output
     * @param lineNumber
     * @return a {@link VariantAnnotation}
     * @throws Exception
     *
     * Most of the code is from org.opencb.biodata.formats.annotation.io.VepFormatReader#read() with few differences:
     *  - An empty array is initialized for Hgvs (like ConsequenceTypes);
     *  - parseFrequencies is always true and the all line is always parsed;
     *  - The logic to move around the file (read line) and reference to previous line (currentVariantString) are removed;
     */
    @Override
    public VariantAnnotation mapLine(String line, int lineNumber) throws Exception {
        //logger.debug("Mapping line {} to VariantAnnotation", line);
        ConsequenceType consequenceType = new ConsequenceType();
        String[] lineFields = line.split("\t");

        Map<String,String> variantMap = parseVariant(lineFields[0], lineFields[1]);  // coordinates and alternative are only parsed once
        VariantAnnotation currentAnnotation = new VariantAnnotation(variantMap.get("chromosome"),
                Integer.valueOf(variantMap.get("start")),
                Integer.valueOf(variantMap.get("end")), variantMap.get("reference"),
                variantMap.get("alternative"));

        // Initialize list of consequence types
        if(currentAnnotation.getConsequenceTypes()==null) {
            currentAnnotation.setConsequenceTypes(new ArrayList<>());
        }

        // Initialize Hgvs
        if(currentAnnotation.getHgvs()==null) {
            currentAnnotation.setHgvs(new ArrayList<>());
        }

        /**
         * parses extra column and populates fields as required. Some lines do not have extra field and end with a \t: the split function above does not return that field
         * true parameter indicates the function to also parse frequencies
         */
        if(lineFields.length>13) {
            parseExtraField(consequenceType, lineFields[13], currentAnnotation);
        }

        // Remaining fields only of interest if the feature is a transcript
        if(lineFields[5].toLowerCase().equals("transcript")) {
            parseRemainingFields(consequenceType, lineFields);
            // Otherwise just set SO terms
        } else {
            consequenceType.setSoTermsFromSoNames(Arrays.asList(lineFields[6].split(",")));   // fill so terms
        }
        currentAnnotation.getConsequenceTypes().add(consequenceType);
        
        return currentAnnotation;
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parseRemainingFields(org.opencb.biodata.models.variant.annotation.ConsequenceType, java.lang.String[])
     */
    private void parseRemainingFields(ConsequenceType consequenceType, String[] lineFields) {
        consequenceType.setEnsemblGeneId(lineFields[3]);    // fill Ensembl gene id
        consequenceType.setEnsemblTranscriptId(lineFields[4]);  // fill Ensembl transcript id
        if(!lineFields[7].equals("-")) {
            consequenceType.setcDnaPosition(parseStringInterval(lineFields[7]));    // fill cdna position
        }
        if(!lineFields[8].equals("-")) {
            consequenceType.setCdsPosition(parseStringInterval(lineFields[8]));  // fill cds position
        }
        if(!lineFields[9].equals("-")) {
            consequenceType.setAaPosition(parseStringInterval(lineFields[9]));    // fill aa position
        }
        consequenceType.setAaChange(lineFields[10]);  // fill aa change
        consequenceType.setCodon(lineFields[11]); // fill codon change
        if(!lineFields[6].equals("") && !lineFields.equals("-")) {  // VEP may leave this field empty
            consequenceType.setSoTermsFromSoNames(Arrays.asList(lineFields[6].split(",")));    // fill so terms
        }
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
            System.out.println("Unexpected format for column 2: "+coordinatesString);
            e.printStackTrace();
            System.exit(1);
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
            System.out.println("Unexpected variant format for column 1: "+variantString);
            e.printStackTrace();
            System.exit(1);
        }

        return parsedVariant;
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parseExtraField(org.opencb.biodata.models.variant.annotation.ConsequenceType, java.lang.String, java.lang.Boolean)
     *
     * The parseFrequencies option has been removed
     */
    private void parseExtraField(ConsequenceType consequenceType, String extraField, VariantAnnotation currentAnnotation) {

        for (String field : extraField.split(";")) {
            String[] keyValue = field.split("=");

            switch (keyValue[0].toLowerCase()) {
                case "aa_maf":
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "ESP_6500",
                                "African_American", currentAnnotation));

                    break;
                case "afr_maf":
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "1000GENOMES",
                                "phase_1_AFR", currentAnnotation));

                    break;
                case "amr_maf":
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "1000GENOMES",
                                "phase_1_AMR", currentAnnotation));

                    break;
                case "asn_maf":
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "1000GENOMES",
                                "phase_1_ASN", currentAnnotation));

                    break;
                case "biotype":
                    consequenceType.setBiotype(keyValue[1]);
                    break;
//                case "canonical":
//                    variantEffect.setCanonical(keyValue[1].equalsIgnoreCase("YES") || keyValue[1].equalsIgnoreCase("Y"));
//                    break;
//                case "ccds":
//                    variantEffect.setCcdsId(keyValue[1]);
//                    break;
//                case "cell_type":
//                    variantAnnotation.getRegulatoryEffect().setCellType(keyValue[1]);
//                    break;
//                case "clin_sig":
//                    variantEffect.setClinicalSignificance(keyValue[1]);
//                    break;
//                case "distance":
//                    variantEffect.setVariantToTranscriptDistance(Integer.parseInt(keyValue[1]));
//                    break;
//                case "domains":
//                    variantEffect.setProteinDomains(keyValue[1].split(","));
//                    break;
                case "ea_maf":
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "ESP_6500",
                                "European_American", currentAnnotation));

                    break;
//                case "ensp":
//                    variantEffect.setProteinId(keyValue[1]);
//                    break;
                case "eur_maf":
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "1000GENOMES",
                                "phase_1_EUR", currentAnnotation));

                    break;
//                case "exon":
//                    variantEffect.setExonNumber(keyValue[1]);
//                    break;
                case "gmaf": // Format is GMAF=G:0.2640  or  GMAF=T:0.1221,-:0.0905
                        if(currentAnnotation.getPopulationFrequencies()==null) {
                            currentAnnotation.setPopulationFrequencies(new ArrayList<>());
                        }
                        currentAnnotation.getPopulationFrequencies().add(parsePopulationFrequency(keyValue[1], "1000GENOMES",
                                "phase_1_ALL", currentAnnotation));

                    break;
                case "hgvsc":
                    if(currentAnnotation.getHgvs()==null) {
                        currentAnnotation.setHgvs(new ArrayList<String>());
                    }
                    currentAnnotation.getHgvs().add(keyValue[1]);
                    break;
                case "hgvsp":
                    if(currentAnnotation.getHgvs()==null) {
                        currentAnnotation.setHgvs(new ArrayList<String>());
                    }
                    currentAnnotation.getHgvs().add(keyValue[1]);
                    break;
//                case "high_inf_pos":
//                    variantAnnotation.getRegulatoryEffect().setHighInformationPosition(keyValue[1].equalsIgnoreCase("YES") || keyValue[1].equalsIgnoreCase("Y"));
//                    break;
//                case "intron":
//                    variantEffect.setIntronNumber(keyValue[1]);
//                    break;
//                case "motif_name":
//                    variantAnnotation.getRegulatoryEffect().setMotifName(keyValue[1]);
//                    break;
//                case "motif_pos":
//                    variantAnnotation.getRegulatoryEffect().setMotifPosition(Integer.parseInt(keyValue[1]));
//                    break;
//                case "motif_score_change":
//                    variantAnnotation.getRegulatoryEffect().setMotifScoreChange(Float.parseFloat(keyValue[1]));
//                    break;
                case "polyphen": // Format is PolyPhen=possibly_damaging(0.859)
                    consequenceType.addProteinSubstitutionScore(parseProteinSubstitutionScore("Polyphen", keyValue[1]));
                    break;
//                case "pubmed":
//                    variantEffect.setPubmed(keyValue[1].split(","));
//                    break;
                case "sift": // Format is SIFT=tolerated(0.07)
                    consequenceType.addProteinSubstitutionScore(parseProteinSubstitutionScore("Sift", keyValue[1]));
                    break;
                case "strand":
                    consequenceType.setStrand(keyValue[1].equals("1")?"+":"-");
                    break;
//                case "sv":
//                    variantEffect.setStructuralVariantsId(keyValue[1].split(","));
//                    break;
                case "symbol":
                    consequenceType.setGeneName(keyValue[1]);
                    break;
//                case "symbol_source":
//                    variantEffect.setGeneNameSource(keyValue[1]);
//                    break;
                default:
                    // ALLELE_NUM, FREQS, IND, ZYG
                    break;
            }
        }
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parsePopulationFrequency(java.lang.String, java.lang.String, java.lang.String)
     */
    private PopulationFrequency parsePopulationFrequency(
            String frequencyStrings, String study, String population, VariantAnnotation currentAnnotation) {
        PopulationFrequency populationFrequency = new PopulationFrequency();
        populationFrequency.setStudy(study);
        populationFrequency.setPop(population);
        populationFrequency.setSuperPop(population);
        populationFrequency.setRefAllele(currentAnnotation.getReferenceAllele());
        populationFrequency.setAltAllele(currentAnnotation.getAlternativeAllele());
        for(String frequencyString : frequencyStrings.split(",")) {
            String[] parts = frequencyString.split(":");
            if (parts[0].equals(currentAnnotation.getAlternativeAllele())) {
                populationFrequency.setAltAlleleFreq(Float.valueOf(parts[1]));
            } else {
                populationFrequency.setRefAlleleFreq(Float.valueOf(parts[1]));
            }
        }

        return populationFrequency;
    }

    /**
     * From org.opencb.biodata.formats.annotation.io.VepFormatReader
     * #parseProteinSubstitutionScore(java.lang.String, java.lang.String)
     */
    private Score parseProteinSubstitutionScore(String predictorName, String scoreString) {
        String[] scoreFields = scoreString.split("[\\(\\)]");
        return new Score(Double.valueOf(scoreFields[1]), predictorName, scoreFields[0]);
    }
}
