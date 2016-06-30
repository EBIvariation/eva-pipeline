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
package embl.ebi.variation.eva.pipeline.annotation;

/**
 * @author Diego Poggioli
 *
 * See http://www.ensembl.org/info/docs/tools/vep/vep_formats.html for an explanation of the format
 */
public class VepInputLine {
    private int chr;
    private int start;
    private int end;
    private String ref;
    private String alt;
    private String type;
    private String refAlt;
    private String strand;
    private int length;

    public int getChr() {
        return chr;
    }

    public void setChr(int chr) {
        this.chr = chr;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
        this.length = Math.max(ref.length(), alt.length());
    }

    public String getAlt() {
        return alt;
    }

    public void setAlt(String alt) {
        this.alt = alt;
        this.length = Math.max(ref.length(), alt.length());
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRefAlt() {
        return refAlt;
    }

    public void setRefAlt(String refAlt) {
        this.refAlt = refAlt;
    }

    public String getStrand() {
        return strand;
    }

    public void setStrand(String strand) {
        this.strand = strand;
    }

    /**
     * @see org.opencb.biodata.models.variant.Variant#transformToEnsemblFormat
     *
     */
    public void transformToEnsemblFormat() {
        if ("INDEL".equals(type) || "SV".equals(type) || length > 1) {
            if (!ref.isEmpty() && !alt.isEmpty() && ref.charAt(0) == alt.charAt(0)) {
                ref = ref.substring(1);
                alt = alt.substring(1);
                start++;
            }

            // opencb sets: end = start + max(referenceAllele.length, alternateAllele.length) -1
            // ensembl sets: end = start + reference.length -1
            end = start + ref.length() -1;    // -1 because the range is inclusive: [start, end]

            if (ref.length() < alt.length()) {  // insertion
                // and ensembl in insertions sets: start = end+1
                start = end + 1;
            }

            length = ref.length();

            if (ref.equals("")) {
                ref = "-";
            }
            if (alt.equals("")) {
                alt = "-";
            }
        }
    }
}
