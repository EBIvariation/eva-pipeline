package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.pipeline.io.contig.ContigMapping;
import uk.ac.ebi.eva.pipeline.io.contig.ContigNaming;
import uk.ac.ebi.eva.pipeline.io.contig.ContigSynonyms;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Converts the contig to its GenBank synonym when possible. If the synonym can't be determined it keeps the contig as
 * is
 */
public class ContigToGenbankReplacerProcessor implements ItemProcessor<Variant, Variant> {
    private static final Logger logger = LoggerFactory.getLogger(ContigToGenbankReplacerProcessor.class);

    public static final String ORIGINAL_CHROMOSOME = "CHR";

    private ContigMapping contigMapping;
    private ContigNaming contigNaming;

    public ContigToGenbankReplacerProcessor(ContigMapping contigMapping, ContigNaming contigNaming) {
        this.contigMapping = contigMapping;
        this.contigNaming = contigNaming;
    }

    @Override
    public Variant process(Variant variant) throws Exception {
        if (contigNaming.equals(ContigNaming.NO_REPLACEMENT)) {
            return variant;
        }

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

        Collection<VariantSourceEntry> sourceEntries = variant.getSourceEntries().values().stream()
                .peek(e -> e.addAttribute(ORIGINAL_CHROMOSOME, variant.getChromosome()))
                .collect(Collectors.toList());

        if (sourceEntries.isEmpty()) {
            throw new IllegalArgumentException("This class can only process variants with at least 1 source entry. "
                    + "Otherwise, the original (replaced) chromosome is lost.");
        }

        sourceEntries.forEach(sourceEntry -> newVariant.addSourceEntry(sourceEntry));
        return newVariant;
    }
}


