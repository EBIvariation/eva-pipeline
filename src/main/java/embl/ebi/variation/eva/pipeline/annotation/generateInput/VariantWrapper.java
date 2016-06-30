package embl.ebi.variation.eva.pipeline.annotation.generateInput;

import org.opencb.biodata.models.variant.Variant;

/**
 * Created by diego on 29/06/2016.
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
