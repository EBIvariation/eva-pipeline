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

import uk.ac.ebi.eva.t2d.model.T2dAnnotation;
import uk.ac.ebi.eva.t2d.utils.VariantUtils;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

@Entity
@Table(name = "common_dv1")
public class VariantInfo {

    @Id
    @Column(name = "VAR_ID")
    private String variantId;

    @Column(name = "CLOSEST_GENE")
    private String closestGene;

    @Column(name = "DBSNP_ID")
    private String dbsnpId;

    @Column(name = "CHROM")
    private String chromosome;

    @Column(name = "POS")
    private Integer position;

    @Column(name = "Reference_Allele")
    private String referenceAllele;

    @Column(name = "Effect_Allele")
    private String effectAllele;

    @Lob
    @Column(name = "TRANSCRIPT_ANNOT", columnDefinition = "LONGTEXT")
    private String transcriptAnnot;

    @Column(name = "GENE")
    private String gene;

    @Column(name = "Condel_PRED")
    private String condelPred;

    @Column(name = "Consequence")
    private String consequence;

    @Column(name = "PolyPhen_PRED")
    private String polyPhenPred;

    @Column(name = "Protein_change")
    private String proteinChange;

    @Column(name = "SIFT_PRED")
    private String siftPred;

    @Column(name = "MOST_DEL_SCORE")
    private Integer mostDelScore;

    @Column(name = "DISPLAY_NAME")
    private String displayName;

    public VariantInfo() {

    }

    public VariantInfo(T2dAnnotation annotation) {
        variantId = VariantUtils.generateVariantId(annotation.getChromosome(), annotation.getStart(), annotation
                .getReference(), annotation.getAlternate());
        dbsnpId = annotation.getDbsnpId();
        chromosome = annotation.getChromosome();
        position = annotation.getStart();
        referenceAllele = annotation.getReference();
        effectAllele = annotation.getAlternate();
        transcriptAnnot = annotation.getTranscriptAnnot();
        gene = annotation.getGene();
        // condelPred =
        consequence = annotation.getConsequence();
        polyPhenPred = annotation.getPolyphen();
        //proteinChange;
        siftPred = annotation.getSift();
        //mostDelScore ;
        //displayName;
    }

    public void setVariantId(String variantId) {
        this.variantId = variantId;
    }

    public void setClosestGene(String closestGene) {
        this.closestGene = closestGene;
    }

    public void setDbsnpId(String dbsnpId) {
        this.dbsnpId = dbsnpId;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public void setReferenceAllele(String referenceAllele) {
        this.referenceAllele = referenceAllele;
    }

    public void setEffectAllele(String effectAllele) {
        this.effectAllele = effectAllele;
    }

    public void setGene(String gene) {
        this.gene = gene;
    }

    public void setConsequence(String consequence) {
        this.consequence = consequence;
    }

    public void setPolyPhenPred(String polyPhenPred) {
        this.polyPhenPred = polyPhenPred;
    }

    public void setSiftPred(String siftPred) {
        this.siftPred = siftPred;
    }

    public String getVariantId() {
        return variantId;
    }

    public String getClosestGene() {
        return closestGene;
    }

    public String getDbsnpId() {
        return dbsnpId;
    }

    public String getChromosome() {
        return chromosome;
    }

    public Integer getPosition() {
        return position;
    }

    public String getReferenceAllele() {
        return referenceAllele;
    }

    public String getEffectAllele() {
        return effectAllele;
    }

    public String getGene() {
        return gene;
    }

    public String getConsequence() {
        return consequence;
    }

    public String getPolyPhenPred() {
        return polyPhenPred;
    }

    public String getSiftPred() {
        return siftPred;
    }
}
