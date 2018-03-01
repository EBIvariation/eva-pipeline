/*
 * Copyright 2014-2016 EMBL - European Bioinformatics Institute
 * Copyright 2015 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.max;

/**
 * Class that parses VCF lines to create Variants.
 */
public class VariantVcfFactory {

    /**
     * Creates a list of Variant objects using the fields in a record of a VCF
     * file. A new Variant object is created per allele, so several of them can
     * be created from a single line.
     * <p>
     * Start/end coordinates assignment tries to work as similarly as possible
     * as Ensembl does, except for insertions, where start is greater than end:
     * http://www.ensembl.org/info/docs/tools/vep/vep_formats.html#vcf
     *
     * @param fileId,
     * @param studyId
     * @param line Contents of the line in the file
     * @return The list of Variant objects that can be created using the fields from a VCF record
     */
    public List<Variant> create(String fileId, String studyId,
                                String line) throws IllegalArgumentException, NotAVariantException {
        String[] fields = line.split("\t");
        if (fields.length < 8) {
            throw new IllegalArgumentException("Not enough fields provided (min 8)");
        }

        String chromosome = getChromosomeWithoutPrefix(fields);
        int position = getPosition(fields);
        Set<String> ids = new HashSet<>(); //EVA-942 - Ignore IDs submitted through VCF
        String reference = getReference(fields);
        String[] alternateAlleles = getAlternateAlleles(fields, chromosome, position, reference);
        float quality = getQuality(fields);
        String filter = getFilter(fields);
        String info = getInfo(fields);
        String format = getFormat(fields);

        List<VariantKeyFields> generatedKeyFields = buildVariantKeyFields(chromosome, position, reference,
                alternateAlleles);

        List<Variant> variants = new LinkedList<>();
        // Now create all the Variant objects read from the VCF record
        for (int altAlleleIdx = 0; altAlleleIdx < alternateAlleles.length; altAlleleIdx++) {
            VariantKeyFields keyFields = generatedKeyFields.get(altAlleleIdx);
            Variant variant = new Variant(chromosome, keyFields.start, keyFields.end, keyFields.reference,
                                          keyFields.alternate);
            String[] secondaryAlternates = getSecondaryAlternates(keyFields.getNumAllele(), alternateAlleles);
            VariantSourceEntry file = new VariantSourceEntry(fileId, studyId, secondaryAlternates, format);
            variant.addSourceEntry(file);

            try {
                parseSplitSampleData(variant, fileId, studyId, fields, alternateAlleles, secondaryAlternates, altAlleleIdx);
                // Fill the rest of fields (after samples because INFO depends on them)
                setOtherFields(variant, fileId, studyId, ids, quality, filter, info, format, keyFields.getNumAllele(),
                               alternateAlleles, line);
                variants.add(variant);
            } catch (NonStandardCompliantSampleField ex) {
                Logger.getLogger(VariantFactory.class.getName())
                      .log(Level.SEVERE,
                           String.format("Variant %s:%d:%s>%s will not be saved\n%s", chromosome, position, reference,
                                         alternateAlleles[altAlleleIdx], ex.getMessage()));
            }
        }

        return variants;
    }

    /**
     * Replace "chr" references only at the beginning of the chromosome name.
     * For instance, tomato has SL2.40ch00 and that should be kept that way
     */
    private String getChromosomeWithoutPrefix(String[] fields) {
        String chromosome = fields[0];
        boolean ignoreCase = true;
        int startOffset = 0;
        String prefixToRemove = "chr";
        if (chromosome.regionMatches(ignoreCase, startOffset, prefixToRemove, startOffset, prefixToRemove.length())) {
            return chromosome.substring(prefixToRemove.length());
        }
        return chromosome;
    }

    private int getPosition(String[] fields) {
        return Integer.parseInt(fields[1]);
    }

    private Set<String> getIds(String[] fields) {
        Set<String> ids = new HashSet<>();
        if (!fields[2].equals(".")) {    // note!: we store a "." as an empty set, not a set with an empty string
            ids.addAll(Arrays.asList(fields[2].split(";")));
        }
        return ids;
    }

    private String getReference(String[] fields) {
        return fields[3].equals(".") ? "" : fields[3];
    }

    private String[] getAlternateAlleles(String[] fields, String chromosome, int position, String reference) {
        return fields[4].split(",");
    }

    private float getQuality(String[] fields) {
        return fields[5].equals(".") ? -1 : Float.parseFloat(fields[5]);
    }

    private String getFilter(String[] fields) {
        return fields[6].equals(".") ? "" : fields[6];
    }

    private String getInfo(String[] fields) {
        return fields[7].equals(".") ? "" : fields[7];
    }

    private String getFormat(String[] fields) {
        return (fields.length <= 8 || fields[8].equals(".")) ? "" : fields[8];
    }

    private List<VariantKeyFields> buildVariantKeyFields(String chromosome, int position, String reference,
            String[] alternateAlleles) {
        List<VariantKeyFields> generatedKeyFields = new ArrayList<>();

        for (int i = 0; i < alternateAlleles.length; i++) { // This index is necessary for getting the samples where the mutated allele is present
            VariantKeyFields keyFields = normalizeLeftAlign(chromosome, position, reference, alternateAlleles[i]);
            keyFields.setNumAllele(i);

            // Since the reference and alternate alleles won't necessarily match
            // the ones read from the VCF file but they are still needed for
            // instantiating the variants, they must be updated
            alternateAlleles[i] = keyFields.alternate;
            generatedKeyFields.add(keyFields);
        }
        return generatedKeyFields;
    }

    /**
     * Calculates the normalized start, end, reference and alternate of a variant where the
     * reference and the alternate are not identical.
     * <p>
     * This task comprises 2 steps: removing the trailing bases that are
     * identical in both alleles, then the leading identical bases.
     * <p>
     * It is left aligned because the traling bases are removed before the leading ones, implying a normalization where
     * the position is moved the least possible from its original location.
     * @param chromosome needed for error reporting and logging
     * @param position Input starting position
     * @param reference Input reference allele
     * @param alternate Input alternate allele
     * @return The new start, end, reference and alternate alleles wrapped in a VariantKeyFields
     */
    protected VariantKeyFields normalizeLeftAlign(String chromosome, int position, String reference, String alternate)
            throws NotAVariantException {
        if (reference.equals(alternate)) {
            throw new NotAVariantException("One alternate allele is identical to the reference. Variant found as: "
                        + chromosome + ":" + position + ":" + reference + ">" + alternate);
        }

        // Remove the trailing bases
        String refReversed = StringUtils.reverse(reference);
        String altReversed = StringUtils.reverse(alternate);
        int indexOfDifference = StringUtils.indexOfDifference(refReversed, altReversed);
        reference = StringUtils.reverse(refReversed.substring(indexOfDifference));
        alternate = StringUtils.reverse(altReversed.substring(indexOfDifference));

        // Remove the leading bases
        indexOfDifference = StringUtils.indexOfDifference(reference, alternate);
        int start = position + indexOfDifference;
        int length = max(reference.length(), alternate.length());
        int end = position + length - 1;    // -1 because end is inclusive
        if (indexOfDifference > 0) {
            reference = reference.substring(indexOfDifference);
            alternate = alternate.substring(indexOfDifference);
        }
        return new VariantKeyFields(start, end, reference, alternate);
    }

    protected String[] getSecondaryAlternates(int numAllele, String[] alternateAlleles) {
        String[] secondaryAlternates = new String[alternateAlleles.length - 1];
        for (int i = 0, j = 0; i < alternateAlleles.length; i++) {
            if (i != numAllele) {
                secondaryAlternates[j++] = alternateAlleles[i];
            }
        }
        return secondaryAlternates;
    }

    protected void parseSplitSampleData(Variant variant, String fileId, String studyId, String[] fields,
                                        String[] alternateAlleles, String[] secondaryAlternates,
                                        int alternateAlleleIdx) throws NonStandardCompliantSampleField,NotAVariantException {
        String[] formatFields = variant.getSourceEntry(fileId, studyId).getFormat().split(":");

        for (int i = 9; i < fields.length; i++) {
        	int ctr=0;
            Map<String, String> map = new TreeMap<>();

            // Fill map of a sample
            String[] sampleFields = fields[i].split(":");

            // Samples may remove the trailing fields (only GT is mandatory),
            // so the loop iterates to sampleFields.length, not formatFields.length
            for (int j = 0; j < sampleFields.length; j++) {
                if (formatFields[j].equals("GT")) {
                    if (sampleFields[j].equals("0|0") || sampleFields[j].equals("0/0") || sampleFields[j].equals("./.")) {
                        ctr++;
            	    }
                }
               
                String formatField = formatFields[j];
                String sampleField = processSampleField(alternateAlleleIdx, formatField, sampleFields[j]);
                map.put(formatField, sampleField);
            }
            if (ctr==sampleFields.length) {
                throw new NotAVariantException("All the sample genotypes are non-variant");
            	}
            else {
                // Add sample to the variant entry in the source file
                variant.getSourceEntry(fileId, studyId).addSampleData(map);
            }
         }
    }

    /**
     * If this is a field other than the genotype (GT), return unmodified. Otherwise,
     * see {@link VariantVcfFactory#processGenotypeField(int, java.lang.String)}
     *
     * @param alternateAlleleIdx current alternate being processed. 0 for first alternate, 1 or more for a secondary alternate.
     * @param formatField as shown in the FORMAT column. most probably the GT field.
     * @param sampleField parsed value in a column of a sample, such as a genotype, e.g. "0/0".
     * @return processed sample field, ready to be stored.
     */
    private String processSampleField(int alternateAlleleIdx, String formatField, String sampleField) {
        if (formatField.equalsIgnoreCase("GT")) {
            return processGenotypeField(alternateAlleleIdx, sampleField);
        } else {
            return sampleField;
        }
    }

    /**
     * Intern the genotype String into the String pool to avoid storing lots of "0/0". In case that the variant is
     * multiallelic and we are currently processing one of the secondary alternates (T is the only secondary alternate
     * in a variant like A -> C,T), change the allele codes to represent the current alternate as allele 1. For details
     * on changing this indexes, see {@link VariantVcfFactory#mapToMultiallelicIndex(int, int)}
     *
     * @param alternateAlleleIdx current alternate being processed. 0 for first alternate, 1 or more for a secondary alternate.
     * @param genotype first field in the samples column, e.g. "0/0"
     * @return the processed genotype string, as described above (interned and changed if multiallelic).
     */
    private String processGenotypeField(int alternateAlleleIdx, String genotype) {
        boolean isNotTheFirstAlternate = alternateAlleleIdx >= 1;
        if (isNotTheFirstAlternate) {
            Genotype parsedGenotype = new Genotype(genotype);

            StringBuilder genotypeStr = new StringBuilder();
            for (int allele : parsedGenotype.getAllelesIdx()) {
                if (allele < 0) { // Missing
                    genotypeStr.append(".");
                } else {
                    // Replace numerical indexes when they refer to another alternate allele
                    genotypeStr.append(String.valueOf(mapToMultiallelicIndex(allele, alternateAlleleIdx)));
                }
                genotypeStr.append(parsedGenotype.isPhased() ? "|" : "/");
            }
            genotype = genotypeStr.substring(0, genotypeStr.length() - 1);
        }

        return genotype.intern();
    }

    protected void setOtherFields(Variant variant, String fileId, String studyId, Set<String> ids, float quality, String filter,
                                  String info, String format, int numAllele, String[] alternateAlleles, String line) {
        // Fields not affected by the structure of REF and ALT fields
        variant.setIds(ids);

        if (quality > -1) {
            variant.getSourceEntry(fileId, studyId)
                   .addAttribute("QUAL", String.valueOf(quality));
        }
        if (!filter.isEmpty()) {
            variant.getSourceEntry(fileId, studyId).addAttribute("FILTER", filter);
        }
        if (!info.isEmpty()) {
            parseInfo(variant, fileId, studyId, info, numAllele);
        }
        variant.getSourceEntry(fileId, studyId).addAttribute("src", line);
    }

    protected void parseInfo(Variant variant, String fileId, String studyId, String info, int numAllele)throws NotAVariantException {
        VariantSourceEntry file = variant.getSourceEntry(fileId, studyId);

        for (String var : info.split(";")) {
            String[] splits = var.split("=");
            if (splits.length == 2) {
                switch (splits[0]) {
                    case "ACC":
                        // Managing accession ID for the allele
                        String[] ids = splits[1].split(",");
                        file.addAttribute(splits[0], ids[numAllele]);
                        break;
                    case "AC":
                        // TODO For now, only one alternate is supported
                        String[] counts = splits[1].split(",");
                        file.addAttribute(splits[0], counts[numAllele]);
                        break;
                    case "AF":
                        // TODO For now, only one alternate is supported
                        if (splits[1].equals("0")) {
                            throw new NotAVariantException();
                        }
                        else{
                            String[] frequencies = splits[1].split(",");
                            file.addAttribute(splits[0], frequencies[numAllele]);
                        }
                        break;
                    case "AN":
                    		 if (splits[1].equals("0")) {
                        		throw new NotAVariantException();
                        	 }
//                        // TODO For now, only two alleles (reference and one alternate) are supported, but this should be changed
//                        file.addAttribute(splits[0], "2");
//                        break;
                    case "NS":
                        // Count the number of samples that are associated with the allele
                        file.addAttribute(splits[0], String.valueOf(file.getSamplesData().size()));
                        break;
                    case "DP":
                        int dp = 0;
                        for (Map<String, String> sampleData : file.getSamplesData()) {
                            String sampleDp = sampleData.get("DP");
                            if (StringUtils.isNumeric(sampleDp)) {
                                dp += Integer.parseInt(sampleDp);
                            }
                        }
                        file.addAttribute(splits[0], String.valueOf(dp));
                        break;
                    case "MQ":
                    case "MQ0":
                        int mq = 0;
                        int mq0 = 0;
                        for (Map<String, String> sampleData : file.getSamplesData()) {
                            String sampleGq = sampleData.get("GQ");
                            if (StringUtils.isNumeric(sampleGq)) {
                                int gq = Integer.parseInt(sampleGq);
                                mq += gq * gq;
                                if (gq == 0) {
                                    mq0++;
                                }
                            }
                        }
                        file.addAttribute("MQ", String.valueOf(mq));
                        file.addAttribute("MQ0", String.valueOf(mq0));
                        break;
                    default:
                        file.addAttribute(splits[0], splits[1]);
                        break;
                }
            } else {
                variant.getSourceEntry(fileId, studyId).addAttribute(splits[0], "");
            }
        }
    }

    protected class VariantKeyFields {

        int start, end, numAllele;

        String reference, alternate;

        public VariantKeyFields(int start, int end, String reference, String alternate) {
            this.start = start;
            this.end = end;
            this.reference = reference;
            this.alternate = alternate;
        }

        public void setNumAllele(int numAllele) {
            this.numAllele = numAllele;
        }

        public int getNumAllele() {
            return numAllele;
        }
    }

    /**
     * In multiallelic variants, we have a list of alternates, where numAllele is the one whose variant we are parsing
     * now. If we are parsing the first variant (numAllele == 0) A1 refers to first alternative, (i.e.
     * alternateAlleles[0]), A2 to second alternative (alternateAlleles[1]), and so on. However, if numAllele == 1, A1
     * refers to second alternate (alternateAlleles[1]), A2 to first (alternateAlleles[0]) and higher alleles remain
     * unchanged. Moreover, if NumAllele == 2, A1 is third alternate, A2 is first alternate and A3 is second alternate.
     * It's also assumed that A0 would be the reference, so it remains unchanged too.
     * <p>
     * This pattern of the first allele moving along (and swapping) is what describes this function. Also, look
     * VariantVcfFactory.getSecondaryAlternates().
     *
     * @param parsedAllele the value of parsed alleles. e.g. 1 if genotype was "A1" (first allele).
     * @param numAllele current variant of the alternates.
     * @return the correct allele index depending on numAllele.
     */
    protected static int mapToMultiallelicIndex(int parsedAllele, int numAllele) {
        int correctedAllele = parsedAllele;
        if (parsedAllele > 0) {
            if (parsedAllele == numAllele + 1) {
                correctedAllele = 1;
            } else if (parsedAllele < numAllele + 1) {
                correctedAllele = parsedAllele + 1;
            }
        }
        return correctedAllele;
    }
}
