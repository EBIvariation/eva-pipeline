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
package uk.ac.ebi.eva.commons.models.data;

import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A mutation in the genome, defined as a change from a reference to an alternate allele in a certain position of
 * said genome.
 */
public class Variant {

    public static final int SV_THRESHOLD = 50;

    /**
     * Type of variation, which depends mostly on its length.
     * <ul>
     * <li>SNVs involve a single nucleotide, without changes in length</li>
     * <li>MNVs involve multiple nucleotides, without changes in length</li>
     * <li>Indels are insertions or deletions of less than SV_THRESHOLD (50) nucleotides</li>
     * <li>Structural variations are large changes of more than SV_THRESHOLD nucleotides</li>
     * <li>Copy-number variations alter the number of copies of a region</li>
     * <li>No alternate alleles found mean that only the reference was reported</li>
     * </ul>
     */
    public enum VariantType {
        SNV, MNV, INDEL, SV, CNV, NO_ALTERNATE
    }

    /**
     * Type of variation
     * @see uk.ac.ebi.eva.commons.models.data.Variant.VariantType
     */
    private VariantType type;

    /**
     * Chromosome where the genomic variation occurred.
     */
    private String chromosome;

    /**
     * Position where the genomic variation starts.
     * <ul>
     * <li>SNVs have the same start and end position</li>
     * <li>Insertions start in the last present position: if the first nucleotide
     * is inserted in position 6, the start is position 5</li>
     * <li>Deletions start in the first previously present position: if the first
     * deleted nucleotide is in position 6, the start is position 6</li>
     * </ul>
     */
    private int start;

    /**
     * Position where the genomic variation ends.
     * <ul>
     * <li>SNVs have the same start and end positions</li>
     * <li>Insertions end in the first present position: if the last nucleotide
     * is inserted in position 9, the end is position 10</li>
     * <li>Deletions ends in the last previously present position: if the last
     * deleted nucleotide is in position 9, the end is position 9</li>
     * </ul>
     */
    private int end;

    /**
     * Length of the genomic variation, which depends on the variation type.
     * <ul>
     * <li>SNVs have a length of 1 nucleotide</li>
     * <li>Indels have the length of the largest allele</li>
     * </ul>
     */
    private int length;

    /**
     * Reference allele.
     */
    private String reference;

    /**
     * Alternate allele.
     */
    private String alternate;

    /**
     * Set of identifiers used for this genomic variation.
     */
    private Set<String> ids;

    /**
     * Unique identifier following the HGVS nomenclature.
     */
    private Map<String, Set<String>> hgvs;

    /**
     * Information specific to each file the variant was read from, such as samples or statistics.
     */
    private Map<String, VariantSourceEntry> sourceEntries;

    /**
     * Annotations of the genomic variation.
     */
    private Annotation annotation;


    public Variant() {
        this("", -1, -1, "", "");
    }

    public Variant(String chromosome, int start, int end, String reference, String alternate) {
        if (start > end && !(reference.equals("-"))) {
            throw new IllegalArgumentException("End position must be greater than the start position");
        }

        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.reference = (reference != null) ? reference : "";
        this.alternate = (alternate != null) ? alternate : "";

        this.length = Math.max(this.reference.length(), this.alternate.length());
        this.resetType();

        this.ids = new HashSet<>();
        this.hgvs = new HashMap<>();
        if (this.type == VariantType.SNV) { // Generate HGVS code only for SNVs
            Set<String> hgvsCodes = new HashSet<>();
            hgvsCodes.add(chromosome + ":g." + start + reference + ">" + alternate);
            this.hgvs.put("genomic", hgvsCodes);
        }

        this.sourceEntries = new HashMap<>();
        this.annotation = new Annotation(this.chromosome, this.start, this.end, this.reference);
    }

    public VariantType getType() {
        return type;
    }

    public void setType(VariantType type) {
        this.type = type;
    }

    private void resetType() {
        if (this.alternate.equals(".")) {
            this.type = VariantType.NO_ALTERNATE;
        } else if (this.reference.length() == this.alternate.length()) {
            if (this.length > 1) {
                this.type = VariantType.MNV;
            } else {
                this.type = VariantType.SNV;
            }
        } else if (this.length <= SV_THRESHOLD) {
            /*
            * 3 possibilities for being an INDEL:
            * - The value of the ALT field is <DEL> or <INS>
            * - The REF allele is not . but the ALT is
            * - The REF allele is . but the ALT is not
            * - The REF field length is different than the ALT field length
            */
            this.type = VariantType.INDEL;
        } else {
            this.type = VariantType.SV;
        }
    }

    public String getChromosome() {
        return chromosome;
    }

    public final void setChromosome(String chromosome) {
        if (chromosome == null || chromosome.length() == 0) {
            throw new IllegalArgumentException("Chromosome must not be empty");
        }
    }

    public int getStart() {
        return start;
    }

    public final void setStart(int start) {
        if (start < 0) {
            throw new IllegalArgumentException("Start must be positive");
        }
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public final void setEnd(int end) {
        if (end < 0) {
            throw new IllegalArgumentException("End must be positive");
        }
        this.end = end;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
        this.length = Math.max(reference.length(), alternate.length());
    }

    public String getAlternate() {
        return alternate;
    }

    public void setAlternate(String alternate) {
        this.alternate = alternate;
        this.length = Math.max(reference.length(), alternate.length());
    }

    public Set<String> getIds() {
        return ids;
    }

    public void setIds(Set<String> ids) {
        this.ids = ids;
    }

    public Map<String, Set<String>> getHgvs() {
        return hgvs;
    }

    public Set<String> getHgvs(String type) {
        return hgvs.get(type);
    }

    public boolean addHgvs(String type, String value) {
        Set<String> listByType = hgvs.get(type);
        if (listByType == null) {
            listByType = new HashSet<>();
        }
        return listByType.add(value);
    }

    public Map<String, VariantSourceEntry> getSourceEntries() {
        return sourceEntries;
    }

    public VariantSourceEntry getSourceEntry(String fileId, String studyId) {
        return sourceEntries.get(composeId(studyId, fileId));
    }

    public void setSourceEntries(Map<String, VariantSourceEntry> sourceEntries) {
        this.sourceEntries = sourceEntries;
    }

    public void addSourceEntry(VariantSourceEntry sourceEntry) {
        this.sourceEntries.put(composeId(sourceEntry.getStudyId(), sourceEntry.getFileId()), sourceEntry);
    }

    public VariantStats getStats(String studyId, String fileId) {
        VariantSourceEntry file = sourceEntries.get(composeId(studyId, fileId));
        if (file == null) {
            return null;
        }
        return file.getStats();
    }

    public Annotation getAnnotation() {
        return annotation;
    }

    public void setAnnotation(Annotation annotation) {
        this.annotation = annotation;
    }

    /**
     * Copies the current variant and returns the copy in Ensembl format.
     * see http://www.ensembl.org/info/docs/tools/vep/vep_formats.html
     * <p>
     * This variant remains unchanged, but the copy is a shallow copy, so any changes to the copy will affect the
     * original as well.
     *
     * @return a modified copy
     */
    public Variant copyInEnsemblFormat() {
        Variant variant = this.clone();
        variant.transformToEnsemblFormat();
        return variant;
    }

    /**
     * see http://www.ensembl.org/info/docs/tools/vep/vep_formats.html
     */
    private void transformToEnsemblFormat() {
        if (type == VariantType.INDEL || type == VariantType.SV || length > 1) {
            if (!reference.isEmpty() && !alternate.isEmpty() && reference.charAt(0) == alternate.charAt(0)) {
                reference = reference.substring(1);
                alternate = alternate.substring(1);
                start++;
            }

            // opencb sets: end = start + max(referenceAllele.length, alternateAllele.length) -1
            // ensembl sets: end = start + reference.length -1
            end = start + reference.length() - 1;    // -1 because the range is inclusive: [start, end]

            if (reference.length() < alternate.length()) {  // insertion
                // and ensembl in insertions sets: start = end+1
                start = end + 1;
            }

            length = reference.length();

            if (reference.equals("")) {
                reference = "-";
            }
            if (alternate.equals("")) {
                alternate = "-";
            }
        }
    }

    @Override
    public String toString() {
        return "Variant{" +
                "chromosome='" + chromosome + '\'' +
                ", position=" + start + "-" + end +
                ", reference='" + reference + '\'' +
                ", alternate='" + alternate + '\'' +
                ", ids='" + ids + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.chromosome);
        hash = 37 * hash + this.start;
        hash = 37 * hash + this.end;
        hash = 37 * hash + Objects.hashCode(this.reference);
        hash = 37 * hash + Objects.hashCode(this.alternate);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Variant other = (Variant) obj;
        if (!Objects.equals(this.chromosome, other.chromosome)) {
            return false;
        }
        if (this.start != other.start) {
            return false;
        }
        if (this.end != other.end) {
            return false;
        }
        if (!Objects.equals(this.reference, other.reference)) {
            return false;
        }
        if (!Objects.equals(this.alternate, other.alternate)) {
            return false;
        }
        return true;
    }

    /**
     * As the clone in the classes Map, Set and Annotation doesn't exist, this is a shallow clone.
     *
     * @return a shallow copy of this variant.
     */
    public Variant clone() {
        Variant variant = new Variant(chromosome, start, end, reference, alternate);
        variant.setAnnotation(this.getAnnotation());
        variant.setIds(this.getIds());
        variant.setSourceEntries(this.getSourceEntries());
        variant.setType(this.getType());
        variant.hgvs = variant.getHgvs();
        variant.setLength(this.getLength());
        return variant;
    }

    private String composeId(String studyId, String fileId) {
        return studyId + "_" + fileId;
    }

}
