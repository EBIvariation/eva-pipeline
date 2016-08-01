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
package embl.ebi.variation.eva.pipeline.annotation.generateInput;

import org.opencb.biodata.models.variant.Variant;

/**
 * @author Diego Poggioli
 *
 * Container for {@link Variant} including strand. By default strand in VCF is always '+'
 */
public class VariantWrapper {

    private Variant variant;
    private String strand="+";

    public VariantWrapper(Variant variant) {
        this.variant = variant.copyInEnsemblFormat();
    }

    public String getChr() {
        return variant.getChromosome();
    }

    public int getStart() {
        return variant.getStart();
    }

    public int getEnd() {
        return variant.getEnd();
    }

    public String getRefAlt() {
        return String.format("%s/%s", variant.getReference(), variant.getAlternate());
    }

    public String getStrand() {
        return strand;
    }

}
