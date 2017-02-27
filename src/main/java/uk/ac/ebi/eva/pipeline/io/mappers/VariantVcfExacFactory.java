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

import org.opencb.biodata.models.feature.Genotype;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Overrides the methods in VariantAggregatedVcfFactory that take care of the fields QUAL, FILTER and INFO, to support
 * the specific format of Exome Aggregation Consortium VCFs.
 */
public class VariantVcfExacFactory extends VariantAggregatedVcfFactory {

    private static final String AC_HOM = "AC_Hom";

    private static final String AC_HET = "AC_Het";

    private static final String AN_ADJ = "AN_Adj";

    private static final String AC_ADJ = "AC_Adj";

    private static final String COMMA = ",";

    private static final String EXAC_MAPPING_FILE = "/mappings/exac-mapping.properties";

    public VariantVcfExacFactory() {
        this(null);
    }

    /**
     * @param tagMap Extends the VariantAggregatedVcfFactory(Properties properties) with two extra tags: Het and Hom.
     * Example:
     * <p>
     * <pre>
     * {@code
     *
     * SAS.AC = AC_SAS
     * SAS.AN = AN_SAS
     * SAS.HET=Het_SAS
     * SAS.HOM=Hom_SAS
     * ALL.AC =AC_Adj
     * ALL.AN =AN_Adj
     * ALL.HET=AC_Het
     * ALL.HOM=AC_Hom
     * }
     * </pre>
     * <p>
     * Het is the list of heterozygous counts as listed by VariantVcfExacFactory.getHeterozygousGenotype() Hom is the
     * list of homozygous counts as listed by VariantVcfExacFactory.getHomozygousGenotype()
     */
    public VariantVcfExacFactory(Properties tagMap) {
        super(tagMap);
    }

    @Override
    protected void loadDefaultMappings() {
        try {
            loadMappings(FileUtils.getPropertiesFile(FileUtils.getResourceAsStream(EXAC_MAPPING_FILE)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void parseStats(Variant variant, String fileId, String studyId, int numAllele, String[] alternateAlleles,
                              String info) {
        VariantSourceEntry sourceEntry = variant.getSourceEntry(fileId, studyId);
        VariantStats stats = new VariantStats(variant);

        if (sourceEntry.hasAttribute(AC_HET)) {   // heterozygous genotype count
            String[] hetCounts = sourceEntry.getAttribute(AC_HET).split(COMMA);
            addHeterozygousGenotypes(variant, numAllele, alternateAlleles, stats, hetCounts);
        }

        if (sourceEntry.hasAttribute(AC_HOM)) {   // homozygous genotype count
            String[] homCounts = sourceEntry.getAttribute(AC_HOM).split(COMMA);
            addHomozygousGenotype(variant, numAllele, alternateAlleles, stats, homCounts);
        }

        String[] acCounts = null;
        if (sourceEntry.hasAttribute(AC_ADJ)) {   // alternative allele counts
            acCounts = sourceEntry.getAttribute(AC_ADJ).split(COMMA);
            if (acCounts.length == alternateAlleles.length) {
                stats.setAltAlleleCount(Integer.parseInt(acCounts[numAllele]));
            }
        }

        if (sourceEntry.hasAttribute(AN_ADJ) && sourceEntry.hasAttribute(AC_ADJ)) {
            // inferring implicit reference allele count
            setRefAlleleCount(stats, Integer.parseInt(sourceEntry.getAttribute(AN_ADJ)),
                              sourceEntry.getAttribute(AC_ADJ).split(COMMA));
        }

        if (sourceEntry.hasAttribute(AC_HOM) && sourceEntry.hasAttribute(AC_HET) && sourceEntry.hasAttribute(AN_ADJ)) {
            // inferring implicit homozygous reference genotype (0/0) count
            int an = Integer.parseInt(sourceEntry.getAttribute(AN_ADJ));
            addReferenceGenotype(variant, stats, an);
        }

        if (sourceEntry.hasAttribute(AC_ADJ) && sourceEntry.hasAttribute(AN_ADJ)) {
            int an = Integer.parseInt(sourceEntry.getAttribute(AN_ADJ));
            setMaf(an, acCounts, alternateAlleles, stats);
        }

        sourceEntry.setStats(stats);
    }


    @Override
    protected void parseCohortStats(Variant variant, String fileId, String studyId, int numAllele, String[] alternateAlleles,
                                    String info) {
        VariantSourceEntry sourceEntry = variant.getSourceEntry(fileId, studyId);
        String[] attributes = info.split(";");
        Map<String, Integer> ans = new LinkedHashMap<>();
        Map<String, String[]> acs = new LinkedHashMap<>();
        for (String attribute : attributes) {
            String[] equalSplit = attribute.split("=");
            if (equalSplit.length == 2) {
                String mappedTag = reverseTagMap.get(equalSplit[0]);
                String[] values = equalSplit[1].split(COMMA);
                if (mappedTag != null) {
                    String[] opencgaTagSplit = mappedTag.split("\\.");   // a literal dot
                    String cohortName = opencgaTagSplit[0];
                    VariantStats cohortStats = sourceEntry.getCohortStats(cohortName);
                    if (cohortStats == null) {
                        cohortStats = new VariantStats(variant);
                        sourceEntry.setCohortStats(cohortName, cohortStats);
                    }
                    switch (opencgaTagSplit[1]) {
                        case "AC":
                            cohortStats.setAltAlleleCount(Integer.parseInt(values[numAllele]));
                            acs.put(cohortName, values);
                            break;
                        case "AN":
                            ans.put(cohortName, Integer.parseInt(values[0]));
                            break;
                        case "HET":
                            addHeterozygousGenotypes(variant, numAllele, alternateAlleles, cohortStats, values);
                            break;
                        case "HOM":
                            addHomozygousGenotype(variant, numAllele, alternateAlleles, cohortStats, values);
                            break;
                    }
                }
            }
        }
        for (String cohortName : sourceEntry.getCohortStats().keySet()) {
            if (ans.containsKey(cohortName)) {
                VariantStats cohortStats = sourceEntry.getCohortStats(cohortName);
                Integer alleleNumber = ans.get(cohortName);
                addReferenceGenotype(variant, cohortStats, alleleNumber);
                setRefAlleleCount(cohortStats, alleleNumber, acs.get(cohortName));
                setMaf(alleleNumber, acs.get(cohortName), alternateAlleles, cohortStats);
            }
        }
    }

    private static void setRefAlleleCount(VariantStats stats, Integer alleleNumber, String alleleCounts[]) {
        int sum = 0;
        for (String ac : alleleCounts) {
            sum += Integer.parseInt(ac);
        }
        stats.setRefAlleleCount(alleleNumber - sum);
    }

    /**
     * Infers the 0/0 genotype count, given that: sum(Heterozygous) + sum(Homozygous) + sum(Reference) = alleleNumber/2
     * <p>
     * TODO Assumes diploid samples, use carefully
     *
     * @param variant to retrieve the alleles to construct the genotype
     * @param stats where to add the 0/0 genotype count
     * @param alleleNumber total sum of alleles.
     */
    private static void addReferenceGenotype(Variant variant, VariantStats stats, int alleleNumber) {
        int gtSum = 0;
        for (Integer gtCounts : stats.getGenotypesCount().values()) {
            gtSum += gtCounts;
        }
        Genotype genotype = new Genotype("0/0", variant.getReference(), variant.getAlternate());
        stats.addGenotype(genotype, alleleNumber / 2 - gtSum);
    }

    /**
     * Adds the heterozygous genotypes to a variant stats. Those are (in this order): 0/1, 0/2, 0/3, 0/4... 1/2, 1/3,
     * 1/4... 2/3, 2/4... 3/4... for a given amount n of alleles, the number of combinations is (latex): \sum_{i=1}^n(
     * \sum_{j=i}^n( 1 ) ), which resolved is n*(n+1)/2
     *
     * @param variant to retrieve the alleles to construct the genotype
     * @param numAllele
     * @param alternateAlleles
     * @param stats where to add the genotypes count
     * @param hetCounts parsed string
     */
    private static void addHeterozygousGenotypes(Variant variant, int numAllele, String[] alternateAlleles,
                                                 VariantStats stats, String[] hetCounts) {
        if (hetCounts.length == alternateAlleles.length * (alternateAlleles.length + 1) / 2) {
            for (int i = 0; i < hetCounts.length; i++) {
                Integer alleles[] = new Integer[2];
                getHeterozygousGenotype(i, alternateAlleles.length, alleles);
                String gt = mapToMultiallelicIndex(alleles[0], numAllele) + "/" + mapToMultiallelicIndex(alleles[1],
                                                                                                         numAllele);
                Genotype genotype = new Genotype(gt, variant.getReference(), alternateAlleles[numAllele]);
                stats.addGenotype(genotype, Integer.parseInt(hetCounts[i]));
            }
        }
    }

    /**
     * Adds the homozygous genotypes to a variant stats. Those are (in this order):
     * 1/1, 2/2, 3/3...
     *
     * @param variant
     * @param numAllele
     * @param alternateAlleles
     * @param stats
     * @param homCounts parsed string
     */
    private void addHomozygousGenotype(Variant variant, int numAllele, String[] alternateAlleles, VariantStats stats,
                                       String[] homCounts) {
        if (homCounts.length == alternateAlleles.length) {
            for (int i = 0; i < homCounts.length; i++) {
                Integer alleles[] = new Integer[2];
                getHomozygousGenotype(i + 1, alleles);
                String gt = mapToMultiallelicIndex(alleles[0], numAllele) + "/" + mapToMultiallelicIndex(alleles[1],
                                                                                                         numAllele);
                Genotype genotype = new Genotype(gt, variant.getReference(), alternateAlleles[numAllele]);
                stats.addGenotype(genotype, Integer.parseInt(homCounts[i]));
            }
        }
    }

    private void setMaf(int totalAlleleCount, String alleleCounts[], String alternateAlleles[], VariantStats stats) {
        if (stats.getMaf() == -1) {

            int referenceCount = stats.getRefAlleleCount();
            float maf = (float) referenceCount / totalAlleleCount;

            String mafAllele = stats.getRefAllele();
            for (int i = 0; i < alleleCounts.length; i++) {
                float auxMaf = (float) Integer.parseInt(alleleCounts[i]) / totalAlleleCount;
                if (auxMaf < maf) {
                    maf = auxMaf;
                    mafAllele = alternateAlleles[i];
                }
            }

            stats.setMaf(maf);
            stats.setMafAllele(mafAllele);
        }
    }

    /**
     * returns in alleles[] the heterozygous genotype specified in index in the sequence (in this example for 3 ALT
     * alleles): 0/1, 0/2, 0/3, 1/2, 1/3, 2/3
     *
     * @param index in this sequence, starting in 0
     * @param numAlternativeAlleles note that this ordering requires knowing how many alleles there are
     * @param alleles returned genotype.
     */
    public static void getHeterozygousGenotype(int index, int numAlternativeAlleles, Integer alleles[]) {
        int cursor = 0;
        for (int i = 0; i < numAlternativeAlleles; i++) {
            for (int j = i + 1; j < numAlternativeAlleles + 1; j++) {
                if (i != j) {
                    if (cursor == index) {
                        alleles[0] = i;
                        alleles[1] = j;
                        return;
                    }
                    cursor++;
                }
            }
        }
    }

    /**
     * returns in alleles[] the homozygous genotype specified in index in the sequence:
     * 0/0, 1/1, 2/2, 3/3
     *
     * @param index in this sequence, starting in 0
     * @param alleles returned genotype.
     */
    public static void getHomozygousGenotype(int index, Integer alleles[]) {
        alleles[0] = alleles[1] = index;
    }
}
