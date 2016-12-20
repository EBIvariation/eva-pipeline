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
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.apache.commons.lang.StringUtils;
import org.beanio.StreamFactory;
import org.beanio.Unmarshaller;
import org.beanio.builder.DelimitedParserBuilder;
import org.beanio.builder.StreamBuilder;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.pipeline.model.Csq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts a {@link Variant} containing a CSQ field into a {@link VariantAnnotation}.
 *
 * {@link Variant} example:
 *
 * ##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID">
 * 1	643359	1_643359_G/A	G	A	.	.	CSQ=A|downstream_gene_variant|MODIFIER||EPlOSAG00000010705|Transcript|EPlOSAT00000012093|ncRNA|||||||||||4191|1|||,A|downstream_gene_variant|MODIFIER||EPlOSAG00000017872|Transcript|EPlOSAT00000019260|ncRNA|||||||||||4428|-1|||,A|upstream_gene_variant|MODIFIER||OS01G0112100|Transcript|OS01T0112100-01|protein_coding|||||||||||4344|-1|||,A|downstream_gene_variant|MODIFIER||OS01G0112201|Transcript|OS01T0112201-01|protein_coding|||||||||||4299|1|||
 *
 * See 'VCF output' in http://www.ensembl.org/info/docs/tools/vep/vep_formats.html
 *
 * Multiple VEP consequences are separated by ','. Data fields are encoded separated by "|";
 * the order of fields is written in the VCF header. Unpopulated fields are represented by an empty string.
 * Consequence field can contain multiple values.
 */
public class VariantToVariantAnnotationProcessor implements ItemProcessor<Variant, VariantAnnotation> {
    protected static Logger logger = LoggerFactory.getLogger(VariantToVariantAnnotationProcessor.class);

    @Override
    public VariantAnnotation process(Variant variant) {
        logger.trace("Converting variant {} into VariantAnnotation", variant);

        Map<String, VariantSourceEntry> sourceEntries = variant.getSourceEntries();

        List<Csq> csqs = new ArrayList<>();

        for (VariantSourceEntry sourceEntry : sourceEntries.values()) {
            String csqValue = sourceEntry.getAttribute("CSQ");
            csqs.addAll(parseCsqValue(csqValue));
        }

        if (!csqs.isEmpty()) {
            VariantAnnotation variantAnnotation = new VariantAnnotation(variant.getChromosome(), variant.getStart(),
                                                                        variant.getEnd(), variant.getReference(),
                                                                        variant.getAlternate());

            variantAnnotation.setConsequenceTypes(extractConsequenceTypesFromCsq(csqs));
            variantAnnotation.setHgvs(extractHgvsFromCsq(csqs));

            return variantAnnotation;
        }

        return null;
    }

    private List<ConsequenceType> extractConsequenceTypesFromCsq(List<Csq> csqs) {
        return csqs.stream().map(this::csqToConsequenceTypeMapper).collect(Collectors.toList());
    }

    private List<String> extractHgvsFromCsq(List<Csq> csqs) {
        return csqs.stream().map(csq -> Arrays.asList(csq.getHgvsC(), csq.getHgvsP())).flatMap(Collection::stream)
                .filter(StringUtils::isNotBlank).collect(Collectors.toList());
    }

    private ConsequenceType csqToConsequenceTypeMapper(Csq csq) {
        ConsequenceType consequenceType = new ConsequenceType();
        consequenceType.setcDnaPosition(csq.getcDNAposition());
        consequenceType.setCdsPosition(csq.getCdsPosition());
        consequenceType.setBiotype(csq.getBiotype());
        consequenceType.setEnsemblGeneId(csq.getGene());
        consequenceType.setEnsemblTranscriptId(csq.getFeature());
        consequenceType.setStrand(csq.getStrand());
        consequenceType.setCodon(csq.getCodons());
        consequenceType.setSoTermsFromSoNames(Arrays.asList(csq.getConsequence().split(",")));

        return consequenceType;
    }

    private List<Csq> parseCsqValue(String csqValue) {
        List<Csq> csqs = new ArrayList<>();

        if (csqValue != null) {
            csqs = Arrays.stream(csqValue.split(",")).map(this::stringToCsqMapper).collect(Collectors.toList());
        }

        return csqs;
    }

    private Csq stringToCsqMapper(String csq) {
        logger.trace("Mapping CSQ field {}", csq);

        StreamFactory factory = StreamFactory.newInstance();
        StreamBuilder builder = new StreamBuilder("csqMapper").format("delimited")
                .parser(new DelimitedParserBuilder('|')).addRecord(Csq.class);
        factory.define(builder);
        Unmarshaller unmarshaller = factory.createUnmarshaller("csqMapper");

        return (Csq) unmarshaller.unmarshal(replaceAmpersandsWithComma(csq));
    }

    /**
     * Commas in fields are replaced with ampersands (&) to preserve VCF format.
     * See http://www.ensembl.org/info/docs/tools/vep/vep_formats.html
     *
     * @param csq value
     * @return csq value with '&' replaced by ','
     */
    private String replaceAmpersandsWithComma(String csq) {
        return csq.replaceAll("&", ",");
    }

}
