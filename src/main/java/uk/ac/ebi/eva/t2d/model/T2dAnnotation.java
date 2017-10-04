package uk.ac.ebi.eva.t2d.model;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;

public class T2dAnnotation {

    private static final Logger logger = LoggerFactory.getLogger(T2dAnnotation.class);

    private String chromosome;

    private Integer start;

    private Integer end;

    private String reference;

    private String alternate;

    private String gene;

    private String transcript;

    private final HashMap<String, String> transcriptFields;

    private String consequence;

    private String dbsnpId;

    private final HashMap<String, String> extraFields;

    private String polyphen;

    private String sift;

    public T2dAnnotation(String[] vepFields) {
        transcriptFields = new HashMap<>();
        extraFields = new HashMap<>();
        parseFirstColumn(vepFields);
        parseTranscriptFields(vepFields);
        parseEnd(vepFields);
        parseSoAccessions(vepFields);
        parseRsAccessions(vepFields);
        parseExtraFieldIfExists(vepFields);
    }

    private void parseFirstColumn(String[] fields) {
        String lineField = fields[0];
        try {
            // Some VEP examples:
            // 1_718787_-/T    1:718786-718787 T    ...
            // 1_718787_T/-    1:718787        -    ...
            // 1_718788_T/A    1:718788        A    ...
            String[] variantFields = lineField.split("[\\/]");
            String[] leftFields = variantFields[0].split("_");

            // Chr id containing _
            if (leftFields.length > 3) {
                String[] chromosomeFields = (String[]) ArrayUtils.subarray(leftFields, 0, leftFields.length - 2);
                chromosome = String.join("_", chromosomeFields);
            } else {
                chromosome = leftFields[0];
            }
            start = parseInt(leftFields[leftFields.length - 2]);
            reference = leftFields[leftFields.length - 1];
            alternate = variantFields[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error("Unexpected variant format for column 1: " + lineField);
            throw e;
        }
    }

    private void parseEnd(String[] lineFields) {
        String coordinates = lineFields[1];
        try {
            String[] variantLocationFields = coordinates.split("[:-]");
            end = parseInt((variantLocationFields.length > 2) ? variantLocationFields[2] : variantLocationFields[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error("Unexpected format for column 2: " + coordinates);
            throw e;
        }
    }

    private void parseTranscriptFields(String[] lineFields) {
        if (lineFields[2].contains("Transcript")) {
            gene = lineFields[3];
            transcript = lineFields[4];

            if (!lineFields[7].equals("-")) {
                transcriptFields.put("cDnaPosition", lineFields[7]);
            }
            if (!lineFields[8].equals("-")) {
                transcriptFields.put("cdsPosition", lineFields[8]);
            }
            if (!lineFields[9].equals("-")) {
                transcriptFields.put("aaPosition", lineFields[9]);
            }
            transcriptFields.put("aaChange", lineFields[10]);
            transcriptFields.put("codon", lineFields[11]);
        }
    }

    private void parseSoAccessions(String[] lineFields) {
        if (!lineFields[6].equals("") && !lineFields[6].equals("-")) {  // VEP may leave this field empty
            consequence = lineFields[6];
        }
    }

    private void parseRsAccessions(String[] lineFields) {
        if (!lineFields[12].equals("-")) {
            dbsnpId = lineFields[7];
        }
    }

    private void parseExtraFieldIfExists(String[] lineFields) {
        if (lineFields.length < 14) {
            return;
        }
        String extraField = lineFields[13];

        for (String field : extraField.split(";")) {
            String[] keyValue = field.split("=");

            switch (keyValue[0].toLowerCase()) {
                case "polyphen": // Format is PolyPhen=possibly_damaging(0.859)
                    polyphen = parseProteinSubstitutionScore(keyValue[1]);
                    break;
                case "sift": // Format is SIFT=tolerated(0.07)
                    sift = parseProteinSubstitutionScore(keyValue[1]);
                    break;
                case "strand":
                    extraFields.put("strand", keyValue[1].equals("1") ? "+" : "-");
                    break;
                default:
                    // ALLELE_NUM, FREQS, IND, ZYG
                    extraFields.put(keyValue[0], keyValue[1]);
                    break;
            }
        }
    }

    private String parseProteinSubstitutionScore(String scoreString) {
        String[] scoreFields = scoreString.split("[\\(\\)]");
        return scoreFields[0];
    }

    public String getChromosome() {
        return chromosome;
    }

    public Integer getStart() {
        return start;
    }

    public Integer getEnd() {
        return end;
    }

    public String getReference() {
        return reference;
    }

    public String getAlternate() {
        return alternate;
    }

    public String getGene() {
        return gene;
    }

    public String getTranscript() {
        return transcript;
    }

    public String getConsequence() {
        return consequence;
    }

    public String getDbsnpId() {
        return dbsnpId;
    }

    public String getPolyphen() {
        return polyphen;
    }

    public String getSift() {
        return sift;
    }

    public String getTranscriptAnnot() {
        String json = Stream.concat(transcriptFields.entrySet().stream(), extraFields.entrySet().stream())
                .map(entry -> "\"" + entry.getKey() + "\": \"" + entry.getValue() + "\"")
                .collect(Collectors.joining(","));
        return "{\"" + transcript + "\": {" + json + "}";
    }
}
