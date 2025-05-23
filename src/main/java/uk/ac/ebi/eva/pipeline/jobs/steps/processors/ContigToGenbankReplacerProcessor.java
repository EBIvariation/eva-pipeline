package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.io.contig.ContigMapping;
import uk.ac.ebi.eva.pipeline.io.contig.ContigSynonyms;

/**
 * Converts the contig to its GenBank synonym when possible. If the synonym can't be determined it keeps the contig as
 * is
 */
public class ContigToGenbankReplacerProcessor implements ItemProcessor<Variant, Variant> {
    private static final Logger logger = LoggerFactory.getLogger(ContigToGenbankReplacerProcessor.class);

    private ContigMapping contigMapping;

    public ContigToGenbankReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public Variant process(Variant variant) throws Exception {
        String contigName = variant.getChromosome();
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName);

        StringBuilder message = new StringBuilder();
        if (contigMapping.isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            return replaceContigWithGenbankAccession(variant, contigSynonyms);
        } else {
            throw new IllegalArgumentException(message.toString());
        }
    }

    private Variant replaceContigWithGenbankAccession(Variant variant, ContigSynonyms contigSynonyms) {
        Variant newVariant = new Variant(contigSynonyms.getGenBank(), variant.getStart(), variant.getEnd(),
                variant.getReference(), variant.getAlternate());
        newVariant.setSourceEntries(variant.getSourceEntries());
        return newVariant;
    }
}


