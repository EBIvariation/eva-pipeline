package uk.ac.ebi.eva.t2d.jobs.processors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.t2d.services.T2dService;

public class ExistingVariantFilter implements ItemProcessor<Variant, EnsemblVariant> {

    @Autowired
    private T2dService service;


    @Override
    public EnsemblVariant process(Variant variant) throws Exception {
        if (service.exists(variant)) {
            // Variant already exists, skip annotation and loading
            return null;
        } else {
            return new EnsemblVariant(variant.getChromosome(), variant.getStart(), variant.getEnd(),
                    variant.getReference(), variant.getAlternate());
        }
    }
}
