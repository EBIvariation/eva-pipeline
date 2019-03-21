/*
 * Copyright 2014-2016 EMBL - European Bioinformatics Institute
 * Copyright 2015 OpenCB
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
package uk.ac.ebi.eva.pipeline.io.mappers;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Overrides the methods in VariantAggregatedVcfFactory that take care of the fields QUAL, FILTER and INFO, to support
 * the specific format of Exome Variant Server VCFs.
 */
public class VariantVcfEVSFactory extends VariantAggregatedVcfFactory {

    private static final String EVS_MAPPING_FILE = "/mappings/evs-mapping.properties";

    public VariantVcfEVSFactory() {
        this(null);
    }

    /**
     * @param tagMap Extends the VariantAggregatedVcfFactory(Properties properties) with one extra tag: GROUPS_ORDER.
     * Example:
     * <pre>
     * {@code
     *
     * EUR.AF=EUR_AF
     * EUR.AC=AC_EUR
     * EUR.AN=EUR_AN
     * EUR.GTC=EUR_GTC
     * ALL.AF=AF
     * ALL.AC=TAC
     * ALL.AN=AN
     * ALL.GTC=GTC
     * GROUPS_ORDER=EUR,ALL
     * }
     * </pre>
     * <p>
     * The special tag 'GROUPS_ORDER' can be used to specify the order of the comma separated values for populations in
     * tags such as MAF.
     */
    public VariantVcfEVSFactory(Properties tagMap) {
        super(tagMap);
    }

    @Override
    protected void loadDefaultMappings() {
        try {
            loadMappings(FileUtils.getPropertiesFile(FileUtils.getResourceAsStream(EVS_MAPPING_FILE)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void setOtherFields(Variant variant, String fileId, String studyId, Set<String> ids, float quality,
                                  String filter, String info, String format, int numAllele, String[] alternateAlleles,
                                  String line) {
        // Fields not affected by the structure of REF and ALT fields
        variant.setIds(ids);
        VariantSourceEntry sourceEntry = variant.getSourceEntry(fileId, studyId);
        if (quality > -1) {
            sourceEntry.addAttribute("QUAL", String.valueOf(quality));
        }
        if (!filter.isEmpty()) {
            sourceEntry.addAttribute("FILTER", filter);
        }
        if (!info.isEmpty()) {
            parseInfo(variant, fileId, studyId, info, numAllele);
        }
        sourceEntry.setFormat(format);
        sourceEntry.addAttribute("src", line);


        if (tagMap == null) {   // whether we can parse population stats or not
            parseEVSAttributes(variant, fileId, studyId, numAllele, alternateAlleles);
        } else {
            parseCohortEVSInfo(variant, sourceEntry, numAllele, alternateAlleles);
        }
    }

    private void parseEVSAttributes(Variant variant, String fileId, String studyId, int numAllele, String[] alternateAlleles) {
        VariantSourceEntry file = variant.getSourceEntry(fileId, studyId);
        VariantStats stats = new VariantStats(variant);
        if (file.hasAttribute("MAF")) {
            String splitsMAF[] = file.getAttribute("MAF").split(",");
            if (splitsMAF.length == 3) {
                float maf = Float.parseFloat(splitsMAF[2]) / 100;
                stats.setMaf(maf);
            }
        }

        if (file.hasAttribute("GTS") && file.hasAttribute("GTC")) {
            String splitsGTC[] = file.getAttribute("GTC").split(",");
            addGenotypeWithGTS(variant, file, splitsGTC, alternateAlleles, numAllele, stats);
        }
        file.setStats(stats);
    }


    private void parseCohortEVSInfo(Variant variant, VariantSourceEntry sourceEntry,
                                    int numAllele, String[] alternateAlleles) {
        if (tagMap != null) {
            for (String key : sourceEntry.getAttributes().keySet()) {
                String opencgaTag = reverseTagMap.get(key);
                String[] values = sourceEntry.getAttribute(key).split(",");
                if (opencgaTag != null) {
                    String[] opencgaTagSplit = opencgaTag.split("\\."); // a literal point
                    if (opencgaTagSplit.length == 2) {
                        String cohort = opencgaTagSplit[0];
                        VariantStats cohortStats = sourceEntry.getCohortStats(cohort);
                        if (cohortStats == null) {
                            cohortStats = new VariantStats(variant);
                            sourceEntry.setCohortStats(cohort, cohortStats);
                        }
                        switch (opencgaTagSplit[1]) {
                            case "AC":
                                cohortStats.setAltAlleleCount(Integer.parseInt(values[numAllele]));
                                cohortStats.setRefAlleleCount(Integer.parseInt(
                                        values[values.length - 1]));    // ref allele count is the last one
                                break;
                            case "AF":
                                cohortStats.setAltAlleleFreq(Float.parseFloat(values[numAllele]));
                                cohortStats.setRefAlleleFreq(Float.parseFloat(values[values.length - 1]));
                                break;
                            case "AN":
                                // TODO implement this. also, take into account that needed fields may not be processed yet
                                break;
                            case "GTC":
                                addGenotypeWithGTS(variant, sourceEntry, values, alternateAlleles, numAllele,
                                                   cohortStats);
                                break;
                            default:
                                break;
                        }
                    }
                } else if (key.equals("MAF")) {
                    String groups_order = tagMap.getProperty("GROUPS_ORDER");
                    if (groups_order != null) {
                        String[] populations = groups_order.split(",");
                        if (populations.length == values.length) {
                            for (int i = 0; i < values.length; i++) {   // each value has the maf of each population
                                float maf = Float.parseFloat(values[i]) / 100;  // from [0, 100] (%) to [0, 1]
                                VariantStats cohortStats = sourceEntry.getCohortStats(populations[i]);
                                if (cohortStats == null) {
                                    cohortStats = new VariantStats(variant);
                                    sourceEntry.setCohortStats(populations[i], cohortStats);
                                }
                                cohortStats.setMaf(maf);
                            }
                        }
                    }
                }
            }
            // TODO reprocess stats to complete inferable values. A StatsHolder may be needed to keep values not storables in VariantStats
        }
    }

}

