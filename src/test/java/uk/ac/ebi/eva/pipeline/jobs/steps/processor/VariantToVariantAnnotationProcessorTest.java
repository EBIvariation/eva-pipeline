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
package uk.ac.ebi.eva.pipeline.jobs.steps.processor;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.pipeline.io.readers.VcfReader;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.VariantToVariantAnnotationProcessor;
import uk.ac.ebi.eva.test.data.VariantToVariantAnnotationProcessorTestData;
import uk.ac.ebi.eva.test.utils.TestFileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for {@link VariantToVariantAnnotationProcessor}
 */
public class VariantToVariantAnnotationProcessorTest {
    private static final String INPUT_FILE_PATH = "/annotation/vcfWithCsq.vcf";

    private static final String FILE_ID = "5";

    private static final String STUDY_ID = "7";

    private static final String STUDY_NAME = "study name";

    private final List<String> csqFields = Arrays.asList("Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID".split("\\|", -1));

    private VariantToVariantAnnotationProcessor processor;

    private Variant variant;

    private VariantSourceEntry sourceEntry;

    @Before
    public void setUp() throws Exception {
        variant = new Variant("1", 1, 2, "A", "T");
        sourceEntry = new VariantSourceEntry(FILE_ID, STUDY_ID);
    }

    @Test
    public void variantWithoutSourceEntriesShouldReturnNull() {
        processor = new VariantToVariantAnnotationProcessor(new ArrayList<>());

        assertNull(processor.process(variant));
    }

    @Test
    public void sourceEntriesWithoutAttributesShouldReturnNull() {
        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        processor = new VariantToVariantAnnotationProcessor(new ArrayList<>());

        assertNull(processor.process(variant));
    }

    @Test
    public void sourceEntriesWithoutCsqAttributeShouldReturnNull() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("anyKey", "anyValue");
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        processor = new VariantToVariantAnnotationProcessor(new ArrayList<>());

        assertNull(processor.process(variant));
    }

    @Test(expected = RuntimeException.class)
    public void csqInfoFieldsInHeaderMissing() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("CSQ", "anyValue");
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        processor = new VariantToVariantAnnotationProcessor(new ArrayList<>());

        processor.process(variant);
    }

    @Test
    public void sourceEntriesWithOneValidPartialCsqValue() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("CSQ", VariantToVariantAnnotationProcessorTestData.SINGLE_PARTIAL_CSQ);
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        List<String> partialCsqFields = Arrays.asList("Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids".split("\\|", -1));

        processor = new VariantToVariantAnnotationProcessor(partialCsqFields);

        VariantAnnotation variantAnnotation = processor.process(variant);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertNull(variantAnnotation.getHgvs());
        assertEquals(VariantToVariantAnnotationProcessorTestData.SINGLE_PARTIAL_CSQ.split(",").length,
                     consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.get(0);

        assertEquals("EPlOSAG00000001824", consequenceType.getEnsemblGeneId());
        assertEquals("EPlOSAT00000003212", consequenceType.getEnsemblTranscriptId());
        assertEquals("ncRNA", consequenceType.getBiotype());

        ConsequenceType expectedConsequenceType = new ConsequenceType("upstream_gene_variant");

        assertEquals(expectedConsequenceType.getSoTerms().get(0).getSoAccession(),
                     consequenceType.getSoTerms().get(0).getSoAccession());
        assertNull(consequenceType.getStrand());
        assertNull(consequenceType.getcDnaPosition());
        assertNull(consequenceType.getCdsPosition());
        assertNull(consequenceType.getCodon());
    }

    @Test
    public void sourceEntriesWithOneValidFullCsqValue() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("CSQ", VariantToVariantAnnotationProcessorTestData.SINGLE_FULL_CSQ);
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        processor = new VariantToVariantAnnotationProcessor(csqFields);

        VariantAnnotation variantAnnotation = processor.process(variant);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertEquals(VariantToVariantAnnotationProcessorTestData.SINGLE_FULL_CSQ.split(",").length,
                     consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.get(0);

        assertEquals("gene", consequenceType.getEnsemblGeneId());
        assertEquals("feature", consequenceType.getEnsemblTranscriptId());
        assertEquals("strand", consequenceType.getStrand());
        assertEquals("biotype", consequenceType.getBiotype());
        assertEquals(Integer.valueOf(0), consequenceType.getcDnaPosition());
        assertEquals(Integer.valueOf(0), consequenceType.getCdsPosition());
        assertEquals("codons", consequenceType.getCodon());

        ConsequenceType expectedConsequenceType = new ConsequenceType("upstream_gene_variant");

        assertEquals(expectedConsequenceType.getSoTerms().get(0).getSoAccession(),
                     consequenceType.getSoTerms().get(0).getSoAccession());

        assertNull(consequenceType.getGeneName());
        assertNull(consequenceType.getAaPosition());
        assertNull(consequenceType.getAaChange());
        assertNull(consequenceType.getProteinSubstitutionScores());
        assertNull(consequenceType.getRelativePosition());
    }

    @Test
    public void csqFieldsInDifferentOrder() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("CSQ", VariantToVariantAnnotationProcessorTestData.NON_DEFAULT_ORDER_CSQ);
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        List<String> csqFieldsWithDifferentOrder = Arrays.asList("STRAND|Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|cDNA_position|CDS_position|Codons".split("\\|",-1));

        processor = new VariantToVariantAnnotationProcessor(csqFieldsWithDifferentOrder);

        VariantAnnotation variantAnnotation = processor.process(variant);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertEquals(VariantToVariantAnnotationProcessorTestData.NON_DEFAULT_ORDER_CSQ.split(",").length,
                     consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.get(0);

        assertEquals("gene", consequenceType.getEnsemblGeneId());
        assertEquals("feature", consequenceType.getEnsemblTranscriptId());
        assertEquals("strand", consequenceType.getStrand());
        assertEquals("biotype", consequenceType.getBiotype());
        assertEquals(Integer.valueOf(0), consequenceType.getcDnaPosition());
        assertEquals(Integer.valueOf(0), consequenceType.getCdsPosition());
        assertEquals("codons", consequenceType.getCodon());

        ConsequenceType expectedConsequenceType = new ConsequenceType("upstream_gene_variant");

        assertEquals(expectedConsequenceType.getSoTerms().get(0).getSoAccession(),
                     consequenceType.getSoTerms().get(0).getSoAccession());

        assertNull(consequenceType.getGeneName());
        assertNull(consequenceType.getAaPosition());
        assertNull(consequenceType.getAaChange());
        assertNull(consequenceType.getProteinSubstitutionScores());
        assertNull(consequenceType.getRelativePosition());
    }

    @Test
    public void sourceEntriesWithMultipleCsq() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("CSQ", VariantToVariantAnnotationProcessorTestData.MULTIPLE_CSQ);
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        processor = new VariantToVariantAnnotationProcessor(csqFields);

        VariantAnnotation variantAnnotation = processor.process(variant);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertEquals(VariantToVariantAnnotationProcessorTestData.MULTIPLE_CSQ.split(",").length,
                     consequenceTypes.size());
    }

    @Test
    public void ampersandsShouldBeReplacedWithComma() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("CSQ", VariantToVariantAnnotationProcessorTestData.CSQ_WITH_AMPERSANDS);
        sourceEntry.setAttributes(attributes);

        Map<String, VariantSourceEntry> sourceEntries = new HashMap<>();
        sourceEntries.put(FILE_ID, sourceEntry);
        variant.setSourceEntries(sourceEntries);

        processor = new VariantToVariantAnnotationProcessor(csqFields);

        VariantAnnotation variantAnnotation = processor.process(variant);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertEquals(VariantToVariantAnnotationProcessorTestData.SINGLE_PARTIAL_CSQ.split(",").length,
                     consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.get(0);

        ConsequenceType.ConsequenceTypeEntry spliceRegionConsequenceType = new ConsequenceType.ConsequenceTypeEntry(
                "splice_region_variant");
        ConsequenceType.ConsequenceTypeEntry synonymousConsequenceType = new ConsequenceType.ConsequenceTypeEntry(
                "synonymous_variant");

        Set<String> expectedSoAccessions = new HashSet<>();
        expectedSoAccessions.add(spliceRegionConsequenceType.getSoAccession());
        expectedSoAccessions.add(synonymousConsequenceType.getSoAccession());

        Set<String> soAccessions = consequenceType.getSoTerms().stream()
                .map(ConsequenceType.ConsequenceTypeEntry::getSoAccession).collect(Collectors.toSet());

        assertEquals(expectedSoAccessions, soAccessions);
    }

    @Test
    public void vcfWithCsqShouldBeParsedCorrectly() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        File input = TestFileUtils.getResource(INPUT_FILE_PATH);

        VariantSource source = new VariantSource(input.getAbsolutePath(), FILE_ID, STUDY_ID, STUDY_NAME,
                                                 VariantStudy.StudyType.COLLECTION,
                                                 VariantSource.Aggregation.NONE);

        VcfReader vcfReader = new VcfReader(source, input);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        List<Variant> variants = vcfReader.read();

        processor = new VariantToVariantAnnotationProcessor(csqFields);

        for (Variant variant : variants) {
            VariantAnnotation annotation = processor.process(variant);
            assertNotNull(annotation.getConsequenceTypes());
        }
    }

}
