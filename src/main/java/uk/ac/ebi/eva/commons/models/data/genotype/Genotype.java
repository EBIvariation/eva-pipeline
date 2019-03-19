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
package uk.ac.ebi.eva.commons.models.data.genotype;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;


public class Genotype {

    private static Logger logger = LoggerFactory.getLogger(Genotype.class);

    private String reference;
    private String alternate;
    private int[] allelesIdx;
    private boolean phased;

    private AllelesCode code;

    private int count;
    protected static final Pattern genotypePattern = Pattern.compile("/|\\|");


    Genotype() {
    }

    public Genotype(String genotype) {
        this(genotype, null, null);
    }

    public Genotype(String genotype, String ref, String alt) {
        this.reference = ref;
        this.alternate = alt;
        this.phased = genotype.contains("|");
        this.count = 0;
        parseGenotype(genotype);
    }


    private void parseGenotype(String genotype) {
        String[] alleles = genotypePattern.split(genotype, -1);

        this.code = alleles.length > 1 ? AllelesCode.ALLELES_OK : AllelesCode.HAPLOID;
        this.allelesIdx = new int[alleles.length];

        for (int i = 0; i < alleles.length; i++) {
            String allele = alleles[i];

            if (allele.equals(".") || allele.equals("-1")) {
                this.code = AllelesCode.ALLELES_MISSING;
                this.allelesIdx[i] = -1;
            } else {
                Integer alleleParsed = Ints.tryParse(allele);
                if (alleleParsed != null) { // Accepts genotypes with form 0/0, 0/1, and so on
                    this.allelesIdx[i] = alleleParsed;
                } else { // Accepts genotypes with form A/A, A/T, and so on
                    if (allele.equalsIgnoreCase(reference)) {
                        this.allelesIdx[i] = 0;
                    } else if (allele.equalsIgnoreCase(alternate)) {
                        this.allelesIdx[i] = 1;
                    } else {
                        if (allele.isEmpty()) {
                            logger.error("Empty allele: REF=" + reference + ",ALT=" + alternate);
                        }
                        this.allelesIdx[i] = 2; // TODO What happens with more than 2 alternate alleles? Difficult situation
                    }
                }

                if (allelesIdx[i] > 1) {
                    this.code = AllelesCode.MULTIPLE_ALTERNATES;
                }
            }
        }
    }

    public String getReference() {
        return reference;
    }

    void setReference(String reference) {
        this.reference = reference;
    }

    public String getAlternate() {
        return alternate;
    }

    void setAlternate(String alternate) {
        this.alternate = alternate;
    }

    public int getAllele(int i) {
        return allelesIdx[i];
    }

    public int[] getAllelesIdx() {
        return allelesIdx;
    }

    public int[] getNormalizedAllelesIdx() {
        int[] sortedAlleles = Arrays.copyOf(allelesIdx, allelesIdx.length);
        Arrays.sort(sortedAlleles);
        return sortedAlleles;
    }

    void setAllelesIdx(int[] allelesIdx) {
        this.allelesIdx = allelesIdx;
    }

    public boolean isAlleleRef(int i) {
        return allelesIdx[i] == 0;
    }

    public boolean isPhased() {
        return phased;
    }

    void setPhased(boolean phased) {
        this.phased = phased;
    }

    public AllelesCode getCode() {
        return code;
    }

    void setCode(AllelesCode code) {
        this.code = code;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void incrementCount(int count) {
        this.count += count;
    }

    public String getGenotypeInfo() {
        StringBuilder value = new StringBuilder(toString());
        value.append(" (REF=").append(reference);
        value.append(", ALT=").append(alternate);
        value.append(")");
        return value.toString();
    }

    /**
     * Each allele is encoded as the ith-power of 10, being i the index where it is placed. Then its value
     * (0,1,2...) is multiplied by that power.
     * <p>
     * Two genotypes with the same alleles but different phase will have different sign. Phased genotypes
     * have positive encoding, whereas unphased ones have negative encoding.
     * <p>
     * For instance, genotype 1/0 would be -10, 1|0 would be 10 and 2/1 would be -21.
     *
     * @return A numerical encoding of the genotype
     */
    public int encode() {
        // TODO Support missing genotypes
        int encoding = 0;
        for (int i = 0; i < allelesIdx.length; i++) {
            encoding += Math.pow(10, allelesIdx.length - i - 1) * allelesIdx[i];
        }

        return isPhased() ? encoding : encoding * (-1);
    }

    public static Genotype decode(int encoding) {
        // TODO Support missing genotypes
        boolean unphased = encoding < 0;
        if (unphased) {
            encoding = Math.abs(encoding);
        }

        // TODO What to do with haploids?
        StringBuilder builder = new StringBuilder(String.format("%02d", encoding));
        for (int i = 0; i < builder.length() - 1; i += 2) {
            builder.insert(i + 1, unphased ? "/" : "|");
        }

        return new Genotype(builder.toString());
    }

    @Override
    public String toString() {
        StringBuilder value = new StringBuilder();
        value.append(allelesIdx[0] >= 0 ? allelesIdx[0] : ".");
        char separator = isPhased() ? '|' : '/';
        for (int i = 1; i < allelesIdx.length; i++) {
            value.append(separator);
            value.append(allelesIdx[i] >= 0 ? allelesIdx[i] : ".");
        }
        return value.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 11 * hash + Objects.hashCode(this.reference);
        hash = 11 * hash + Objects.hashCode(this.alternate);
        hash = 11 * hash + Arrays.hashCode(this.allelesIdx);
        hash = 11 * hash + (this.phased ? 1 : 0);
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
        final Genotype other = (Genotype) obj;
        if (!Objects.equals(this.reference, other.reference)) {
            return false;
        }
        if (!Objects.equals(this.alternate, other.alternate)) {
            return false;
        }
        if (!Arrays.equals(this.allelesIdx, other.allelesIdx)) {
            return false;
        }
        if (this.phased != other.phased) {
            return false;
        }
        return true;
    }

    public String generateDatabaseString() {
        return toString().replace(".", "-1");
    }
}
