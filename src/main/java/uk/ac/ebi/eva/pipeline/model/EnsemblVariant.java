/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.model;

import org.springframework.util.Assert;


/**
 * Container for the ensembl coordinates of a variant including strand. By default strand in VCF is always '+'
 *
 * The definition of the coordinates is:
 *
 * Start: position after the last unmodified base before the variant.
 *
 * End: position before the first unmodified base after the variant. That is, end = start + reference.length -1.
 *
 * Reference and alternate: the alleles, but if anyone is empty, substitute by "-".
 *
 * @see <a href=http://www.ensembl.org/info/docs/tools/vep/vep_formats.html>VEP default format</a>
 */
public class EnsemblVariant {

    private String chromosome;

    private int start;

    private int end;

    private String reference;

    private String alternate;

    private String strand = "+";

    public EnsemblVariant(String chromosome, int start, int end, String reference, String alternate) {
        Assert.hasText(chromosome);
        Assert.notNull(reference);
        Assert.notNull(alternate);
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.reference = reference;
        this.alternate = alternate;
        transformToEnsemblFormat();
    }

    public String getChr() {
        return chromosome;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public String getRefAlt() {
        return String.format("%s/%s", reference, alternate);
    }

    public String getStrand() {
        return strand;
    }

    private void transformToEnsemblFormat() {
        end = start + reference.length() - 1;

        if (reference.isEmpty()) {
            reference = "-";
        }

        if (alternate.isEmpty()) {
            alternate = "-";
        }
    }
}
