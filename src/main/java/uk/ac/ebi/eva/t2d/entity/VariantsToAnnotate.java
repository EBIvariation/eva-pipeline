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
package uk.ac.ebi.eva.t2d.entity;

import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.t2d.model.IVariant;
import uk.ac.ebi.eva.t2d.utils.VariantUtils;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "VARIANTS_TO_ANNOTATE")
public class VariantsToAnnotate implements IVariant {

    @Id
    @Column(name = "ID", length = 191)
    public String id;

    @Column(nullable = false)
    private String chromosome;

    private int start;

    private int end;

    private String referenceAllele;

    private String effectAllele;

    VariantsToAnnotate() {

    }

    public VariantsToAnnotate(EnsemblVariant variant) {
        chromosome = variant.getChr();
        start = variant.getStart();
        end = variant.getEnd();
        referenceAllele = variant.getReference();
        effectAllele = variant.getAlternate();
        id = VariantUtils.generateVariantId(chromosome, start, referenceAllele, effectAllele);
    }

    @Override
    public String getChr() {
        return chromosome;
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    public int getEnd() {
        return end;
    }

    @Override
    public String getRefAlt() {
        return String.format("%s/%s", referenceAllele, effectAllele);
    }

    @Override
    public String getStrand() {
        return "+";
    }
}
