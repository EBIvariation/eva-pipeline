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
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
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

    private final List<String> csqFields;

    public VariantToVariantAnnotationProcessor(List<String> csqFields) {
        this.csqFields = csqFields;
    }

    @Override
    public VariantAnnotation process(Variant variant) {
        logger.trace("Converting variant {} into VariantAnnotation", variant);

        Map<String, VariantSourceEntry> sourceEntries = variant.getSourceEntries();

        List<ConsequenceType> consequenceTypes = new ArrayList<>();

        for (VariantSourceEntry sourceEntry : sourceEntries.values()) {
            String csqValue = sourceEntry.getAttribute("CSQ");
            consequenceTypes = extractConsequenceTypesFromCsqs(splitMultipleCsqValues(csqValue));
        }

        if (!consequenceTypes.isEmpty()) {
            VariantAnnotation variantAnnotation = new VariantAnnotation(variant.getChromosome(), variant.getStart(),
                                                                        variant.getEnd(), variant.getReference(),
                                                                        variant.getAlternate());

            variantAnnotation.setConsequenceTypes(consequenceTypes);

            return variantAnnotation;
        }

        return null;
    }

    /**
     * Commas in fields are replaced with ampersands (&) to preserve VCF format.
     * See http://www.ensembl.org/info/docs/tools/vep/vep_formats.html
     */
    private List<ConsequenceType> extractConsequenceTypesFromCsqs(List<String> csqs) {
        return csqs.stream()
                .map(csq-> csq.replaceAll("&", ","))
                .map(csq->Arrays.asList(csq.split("\\|", -1)))
                .map(this::csqValuesToConsequenceTypeMapper)
                .collect(Collectors.toList());
    }

    private ConsequenceType csqValuesToConsequenceTypeMapper(List<String> csqValues) {

        if(csqValues.size() != csqFields.size()){
            throw new RuntimeException("CSQ fields in INFO header and CSQ values have different size!");
        }

        ConsequenceType consequenceType = new ConsequenceType();

        for(String csqField : csqFields){

            String csqValue = csqValues.get(csqFields.indexOf(csqField));

            if(!csqValue.isEmpty()){
                populateConsequenceType(consequenceType, csqField, csqValue);
            }
        }

        return consequenceType;
    }

    private void populateConsequenceType(ConsequenceType consequenceType, String csqField, String csqValue){
        switch (csqField){
            case "cDNA_position":
                consequenceType.setcDnaPosition(Integer.valueOf(csqValue));
                break;
            case "CDS_position":
                consequenceType.setCdsPosition(Integer.valueOf(csqValue));
                break;
            case "BIOTYPE":
                consequenceType.setBiotype(csqValue);
                break;
            case "Gene":
                consequenceType.setEnsemblGeneId(csqValue);
                break;
            case "Feature":
                consequenceType.setEnsemblTranscriptId(csqValue);
                break;
            case "STRAND":
                consequenceType.setStrand(csqValue);
                break;
            case "Codons":
                consequenceType.setCodon(csqValue);   //// TODO: 08/02/2017 can be multiple???
                break;
            case "Consequence":
                consequenceType.setSoTermsFromSoNames(Arrays.asList(csqValue.split(",")));
                break;
            default:
                logger.info("The CSQ field {} will not be stored.", csqField);
        }
    }

    private List<String> splitMultipleCsqValues(String csqValue) {
        List<String> csqs = new ArrayList<>();

        if (csqValue != null) {
            csqs = Arrays.asList(csqValue.split(","));
        }

        return csqs;
    }

    /*
    //TODO move this into reader?
    private List<String> extractCsqFieldsFromVcfInfoHeader(VariantSourceEntity variantSourceEntity){
        Collection<VcfInfoHeader> vcfInfoHeaders = (Collection<VcfInfoHeader>) variantSourceEntity.getMetadata().get("INFO");
        List<VcfInfoHeader> csqInfo = vcfInfoHeaders.stream().filter(i-> "CSQ".equals(i.getId())).collect(Collectors.toList());

        if(csqInfo.size()==1){
            String csqDescription = csqInfo.get(0).getDescription();
            csqFields = Arrays.asList(csqDescription.replace("Consequence annotations from Ensembl VEP. Format: ","").split("\\|"));
        }else{
            //// TODO: 09/02/2017 complain!!
        }

        return csqFields;
    }*/

}
