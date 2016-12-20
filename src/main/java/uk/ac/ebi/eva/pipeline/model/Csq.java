package uk.ac.ebi.eva.pipeline.model;

import org.beanio.annotation.Field;
import org.beanio.annotation.Record;

/**
 *
 */
@Record
public class Csq {
    @Field(at=0)
    private String allele;
    @Field(at=1)
    private String consequence; //
    @Field(at=2)
    private String impact;
    @Field(at=3)
    private String symbol;  //always empty?
    @Field(at=4)
    private String gene;    //
    @Field(at=5)
    private String featureType;
    @Field(at=6)
    private String feature; //
    @Field(at=7)
    private String biotype; //
    @Field(at=8)
    private String exon;
    @Field(at=9)
    private String intron;
    @Field(at=10)
    private String hgvsC;   //
    @Field(at=11)
    private String hgvsP;   //
    @Field(at=12)
    private Integer cDNAposition;    //
    @Field(at=13)
    private Integer cdsPosition; //
    @Field(at=14)
    private String proteinPosition;
    @Field(at=15)
    private String aminoAcids;
    @Field(at=16)
    private String codons;  //
    @Field(at=17)
    private String existingVariation;
    @Field(at=18)
    private String distance;
    @Field(at=19)
    private String strand;  //
    @Field(at=20)
    private String flags;
    @Field(at=21)
    private String symbolSource;
    @Field(at=22)
    private String hgncIs;

    public String getAllele() {
        return allele;
    }

    public void setAllele(String allele) {
        this.allele = allele;
    }

    public String getConsequence() {
        return consequence;
    }

    public void setConsequence(String consequence) {
        this.consequence = consequence;
    }

    public String getImpact() {
        return impact;
    }

    public void setImpact(String impact) {
        this.impact = impact;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getGene() {
        return gene;
    }

    public void setGene(String gene) {
        this.gene = gene;
    }

    public String getFeatureType() {
        return featureType;
    }

    public void setFeatureType(String featureType) {
        this.featureType = featureType;
    }

    public String getFeature() {
        return feature;
    }

    public void setFeature(String feature) {
        this.feature = feature;
    }

    public String getBiotype() {
        return biotype;
    }

    public void setBiotype(String biotype) {
        this.biotype = biotype;
    }

    public String getExon() {
        return exon;
    }

    public void setExon(String exon) {
        this.exon = exon;
    }

    public String getIntron() {
        return intron;
    }

    public void setIntron(String intron) {
        this.intron = intron;
    }

    public String getHgvsC() {
        return hgvsC;
    }

    public void setHgvsC(String hgvsC) {
        this.hgvsC = hgvsC;
    }

    public String getHgvsP() {
        return hgvsP;
    }

    public void setHgvsP(String hgvsP) {
        this.hgvsP = hgvsP;
    }

    public Integer getcDNAposition() {
        return cDNAposition;
    }

    public void setcDNAposition(Integer cDNAposition) {
        this.cDNAposition = cDNAposition;
    }

    public Integer getCdsPosition() {
        return cdsPosition;
    }

    public void setCdsPosition(Integer cdsPosition) {
        this.cdsPosition = cdsPosition;
    }

    public String getProteinPosition() {
        return proteinPosition;
    }

    public void setProteinPosition(String proteinPosition) {
        this.proteinPosition = proteinPosition;
    }

    public String getAminoAcids() {
        return aminoAcids;
    }

    public void setAminoAcids(String aminoAcids) {
        this.aminoAcids = aminoAcids;
    }

    public String getCodons() {
        return codons;
    }

    public void setCodons(String codons) {
        this.codons = codons;
    }

    public String getExistingVariation() {
        return existingVariation;
    }

    public void setExistingVariation(String existingVariation) {
        this.existingVariation = existingVariation;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getStrand() {
        return strand;
    }

    public void setStrand(String strand) {
        this.strand = strand;
    }

    public String getFlags() {
        return flags;
    }

    public void setFlags(String flags) {
        this.flags = flags;
    }

    public String getSymbolSource() {
        return symbolSource;
    }

    public void setSymbolSource(String symbolSource) {
        this.symbolSource = symbolSource;
    }

    public String getHgncIs() {
        return hgncIs;
    }

    public void setHgncIs(String hgncIs) {
        this.hgncIs = hgncIs;
    }
}
