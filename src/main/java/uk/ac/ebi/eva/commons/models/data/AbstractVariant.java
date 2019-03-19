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
package uk.ac.ebi.eva.commons.models.data;

import java.util.*;

/**
 * Basic abstract implementation of AbstractVariant model with all the common elements for the current models.
 */
public abstract class AbstractVariant implements IVariant {

    public static final int SV_THRESHOLD = 50;

    protected static final String VARIANT_START_COORDINATE_CANNOT_BE_NEGATIVE =
            "Variant start coordinate cannot be negative";

    protected static final String VARIANT_END_COORDINATE_CANNOT_BE_NEGATIVE = "Variant end coordinate cannot be negative";

    protected static final String END_POSITION_MUST_BE_EQUAL_OR_GREATER_THAN_THE_START_POSITION =
            "End position must be equal or greater than the start position";

    /**
     * Chromosome where the genomic variation occurred.
     */
    private final String chromosome;

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
    private long start;

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
    private long end;

    /**
     * Reference allele.
     */
    private String reference;

    /**
     * Alternate allele.
     */
    private String alternate;

    /**
     * Among all the identifiers used for this genomic variation, the one that is considered the most relevant.
     */
    private String mainId;

    /**
     * Set of identifiers used for this genomic variation.
     */
    private final Set<String> ids;

    /**
     * Set of identifiers used for this genomic variation, according to dbsnp.
     * @deprecated this field is temporary for the dbsnp import, use getIds or getMainId instead
     */
    @Deprecated
    private final Set<String> dbsnpIds;

    /**
     * Unique identifier following the HGVS nomenclature.
     */
    private final Map<String, Set<String>> hgvs;

    protected AbstractVariant() {
        this.chromosome = null;
        this.start = -1;
        this.end = -1;
        this.reference = null;
        this.alternate = null;
        this.mainId = null;
        this.ids = new HashSet<>();
        this.dbsnpIds = new HashSet<>();
        this.hgvs = new HashMap<>();
    }

    public AbstractVariant(String chromosome, long start, long end, String reference, String alternate) {
        this(chromosome, start, end, reference, alternate, null);
    }

    public AbstractVariant(String chromosome, long start, long end, String reference, String alternate, String mainId) {
        if (chromosome == null || chromosome.trim().equals("")) {
            throw new IllegalArgumentException("Chromosome name cannot be empty");
        }
        this.chromosome = chromosome;
        this.setCoordinates(start, end);
        this.setReference(reference);
        this.setAlternate(alternate);
        this.mainId = mainId;

        this.ids = new HashSet<>();
        this.dbsnpIds = new HashSet<>();
        this.hgvs = new HashMap<>();
        if (getType() == VariantType.SNV) { // Generate HGVS code only for SNVs
            Set<String> hgvsCodes = new HashSet<>();
            hgvsCodes.add(chromosome + ":g." + start + reference + ">" + alternate);
            this.hgvs.put("genomic", hgvsCodes);
        }
    }

    public int getLength() {
        return Math.max(this.reference.length(), this.alternate.length());
    }

    public VariantType getType() {
        if (this.alternate.equals(".")) {
            return VariantType.NO_ALTERNATE;
        } else if (reference.length() == alternate.length()) {
            if (getLength() > 1) {
                return VariantType.MNV;
            } else {
                return VariantType.SNV;
            }
        } else if (getLength() <= SV_THRESHOLD) {
            /*
            * 3 possibilities for being an INDEL:
            * - The value of the ALT field is <DEL> or <INS>
            * - The REF allele is . but the ALT is not
            * - The REF field length is different than the ALT field length
            */
            return VariantType.INDEL;
        } else {
            return VariantType.SV;
        }
    }

    public String getChromosome() {
        return chromosome;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public String getReference() {
        return reference;
    }

    public String getAlternate() {
        return alternate;
    }

    public String getMainId() {
        return mainId;
    }

    private void setReference(String reference) {
        this.reference = (reference != null) ? reference : "";
    }

    private void setAlternate(String alternate) {
        this.alternate = (alternate != null) ? alternate : "";
    }

    private void setCoordinates(long start, long end) {
        if (start < 0) {
            throw new IllegalArgumentException(VARIANT_START_COORDINATE_CANNOT_BE_NEGATIVE);
        }
        if (end < 0) {
            throw new IllegalArgumentException(VARIANT_END_COORDINATE_CANNOT_BE_NEGATIVE);
        }
        if (end < start) {
            throw new IllegalArgumentException(END_POSITION_MUST_BE_EQUAL_OR_GREATER_THAN_THE_START_POSITION);
        }
        this.start = start;
        this.end = end;
    }

    public void setMainId(String mainId) {
        this.mainId = mainId;
    }

    public void addId(String id) {
        this.ids.add(id);
    }

    public void setIds(Set<String> ids) {
        this.ids.clear();
        this.ids.addAll(ids);
    }

    public Set<String> getIds() {
        return Collections.unmodifiableSet(ids);
    }

    public void addDbsnpId(String id) {
        this.dbsnpIds.add(id);
    }

    public void setDbsnpIds(Set<String> ids) {
        this.dbsnpIds.clear();
        this.dbsnpIds.addAll(ids);
    }

    public Set<String> getDbsnpIds() {
        return Collections.unmodifiableSet(dbsnpIds);
    }

    public boolean addHgvs(String type, String value) {
        Set<String> listByType = hgvs.get(type);
        if (listByType == null) {
            listByType = new HashSet<>();
        }
        return listByType.add(value);
    }

    public Map<String, Set<String>> getHgvs() {
        return Collections.unmodifiableMap(hgvs);
    }

    public void renormalize(long newStart, long newEnd, String newReference, String newAlternate) {
        this.setCoordinates(newStart, newEnd);
        this.setReference(newReference);
        this.setAlternate(newAlternate);
    }

    @Override
    public String toString() {
        return "AbstractVariant{" +
                "chromosome='" + chromosome + '\'' +
                ", position=" + start + "-" + end +
                ", reference='" + reference + '\'' +
                ", alternate='" + alternate + '\'' +
                ", ids='" + ids + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AbstractVariant other = (AbstractVariant) obj;
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

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.chromosome);
        hash = 37 * hash + (int) (start ^ (start >>> 32));
        hash = 37 * hash + (int) (end ^ (end >>> 32));
        hash = 37 * hash + Objects.hashCode(this.reference);
        hash = 37 * hash + Objects.hashCode(this.alternate);
        return hash;
    }
}
