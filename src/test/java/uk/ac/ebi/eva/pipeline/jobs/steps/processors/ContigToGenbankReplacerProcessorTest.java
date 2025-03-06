package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.pipeline.io.contig.ContigMapping;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.pipeline.jobs.steps.processors.ContigToGenbankReplacerProcessor.ORIGINAL_CHROMOSOME;

public class ContigToGenbankReplacerProcessorTest {

    private ContigToGenbankReplacerProcessor processor;

    @Before
    public void setUp() throws Exception {
        String fileString = ContigToGenbankReplacerProcessorTest.class.getResource(
                "/input-files/assembly-report/assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        processor = new ContigToGenbankReplacerProcessor(contigMapping);
    }

    @Test
    public void ContigGenbank() throws Exception {
        Variant variant = buildMockVariant("CM000093.4");
        assertEquals("CM000093.4", processor.process(variant).getChromosome());
    }

    @Test
    public void ContigChrToGenbank() throws Exception {
        Variant variant = buildMockVariant("chr2");
        assertEquals("CM000094.4", processor.process(variant).getChromosome());
    }

    @Test
    public void ContigRefseqToGenbank() throws Exception {
        Variant variant = buildMockVariant("NW_003763476.1");
        assertEquals("CM000093.4", processor.process(variant).getChromosome());
    }

    @Test(expected = IllegalArgumentException.class)
    public void GenbankAndRefseqNotEquivalents() throws Exception {
        Variant variant = buildMockVariant("chr3");
        processor.process(variant);
    }

    @Test
    public void GenbankAndRefseqNotEquivalentsRefseqNotPresent() throws Exception {
        Variant variant = buildMockVariant("chr6");
        assertEquals("CM000096.4", processor.process(variant).getChromosome());
    }

    @Test(expected = IllegalArgumentException.class)
    public void GenbankAndRefseqNotEquivalentsGenbankNotPresent() throws Exception {
        Variant variant = buildMockVariant("chr5");
        processor.process(variant);
    }

    @Test(expected = IllegalArgumentException.class)
    public void GenbankAndRefseqNotEquivalentsNonePresent() throws Exception {
        Variant variant = buildMockVariant("chr7");
        processor.process(variant);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ContigNotFoundInAssemblyReport() throws Exception {
        Variant variant = buildMockVariant("chr");
        processor.process(variant);
    }

    @Test(expected = IllegalArgumentException.class)
    public void NoGenbankDontConvert() throws Exception {
        Variant variant = buildMockVariant("chr4");
        processor.process(variant);
    }

    @Test
    public void keepOriginalChromosomeInInfo() throws Exception {
        String originalChromosome = "chr1";
        Variant variant = buildMockVariant(originalChromosome);

        Set<String> originalChromosomes = processor.process(variant)
                .getSourceEntries().values()
                .stream()
                .map(e -> e.getAttributes().get(ORIGINAL_CHROMOSOME))
                .collect(Collectors.toSet());

        assertEquals(Collections.singleton(originalChromosome), originalChromosomes);
    }

    private Variant buildMockVariant(String originalChromosome) {
        Variant variant = new Variant(originalChromosome, 1, 1, "A", "T");
        variant.addSourceEntry(new VariantSourceEntry("fileId", "studyId"));
        return variant;
    }
}