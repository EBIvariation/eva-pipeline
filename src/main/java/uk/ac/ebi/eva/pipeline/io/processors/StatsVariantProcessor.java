package uk.ac.ebi.eva.pipeline.io.processors;

import com.mongodb.BasicDBObject;
import org.opencb.biodata.models.feature.Genotype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.commons.models.data.VariantStats;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StatsVariantProcessor implements ItemProcessor<VariantDocument, VariantDocument> {
    private static final Logger logger = LoggerFactory.getLogger(StatsVariantProcessor.class);

    public StatsVariantProcessor() {
    }

    @Override
    public VariantDocument process(VariantDocument variant) {
        String ref = variant.getReference();
        String alt = variant.getAlternate();
        Set<VariantStatsMongo> variantStatsSet = new HashSet<>();

        Set<VariantSourceEntryMongo> variantSourceEntrySet = variant.getVariantSources();
        for (VariantSourceEntryMongo variantSourceEntry : variantSourceEntrySet) {
            String studyId = variantSourceEntry.getStudyId();
            String fileId = variantSourceEntry.getFileId();

            BasicDBObject sampleData = variantSourceEntry.getSampleData();
            if (sampleData == null || sampleData.isEmpty()) {
                continue;
            }

            VariantStats variantStats = getVariantStats(ref, alt, sampleData);
            VariantStatsMongo variantStatsMongo = new VariantStatsMongo(studyId, fileId, "ALL", variantStats);

            variantStatsSet.add(variantStatsMongo);
        }

        if (!variantStatsSet.isEmpty()) {
            variant.setStats(variantStatsSet);
        }

        return variant;
    }

    public VariantStats getVariantStats(String ref, String alt, BasicDBObject sampleData) {
        Map<String, Integer> genotypeCountsMap = new HashMap<>();
        Map<String, Integer> alleleCountsMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : sampleData.entrySet()) {
            String genotype = entry.getKey();
            if (genotype.equals("def")) {
                continue;
            }

            int noOfSamples = ((List<Integer>) entry.getValue()).size();
            String[] genotypeParts = genotype.split("\\||/");

            if (Arrays.stream(genotypeParts).anyMatch("."::equals)) {
                genotypeCountsMap.put(".", genotypeCountsMap.getOrDefault(".", 0) + 1);
            } else {
                genotypeCountsMap.put(genotype, noOfSamples);
            }

            for (String genotypePart : genotypeParts) {
                alleleCountsMap.put(genotypePart, alleleCountsMap.getOrDefault(genotypePart, 0) + noOfSamples);
            }
        }

        int missingGenotypes = genotypeCountsMap.getOrDefault(".", 0);
        genotypeCountsMap.remove(".");
        int totalGenotypes = genotypeCountsMap.values().stream().reduce(0, Integer::sum);
        Optional<Map.Entry<String, Integer>> minGenotypeEntry = genotypeCountsMap.entrySet().stream().min(Map.Entry.comparingByValue());
        String minorGenotype = minGenotypeEntry.get().getKey();
        float minorGenotypeFrequency = (float) minGenotypeEntry.get().getValue() / totalGenotypes;
        Map<Genotype, Integer> genotypeCount = genotypeCountsMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> new Genotype(entry.getKey(), ref, alt), entry -> entry.getValue()));

        int missingAlleles = alleleCountsMap.getOrDefault(".", 0);
        alleleCountsMap.remove(".");
        int totalAlleles = alleleCountsMap.values().stream().reduce(0, Integer::sum);
        Optional<Map.Entry<String, Integer>> minAlleleEntry = alleleCountsMap.entrySet().stream().min(Map.Entry.comparingByValue());
        String minorAllele = minAlleleEntry.get().getKey().equals("0") ? ref : alt.split(",")[Integer.parseInt(minAlleleEntry.get().getKey()) - 1];
        float minorAlleleFrequency = (float) minAlleleEntry.get().getValue() / totalAlleles;

        VariantStats variantStats = new VariantStats();
        variantStats.setRefAllele(ref);
        variantStats.setAltAllele(alt);
        variantStats.setMissingGenotypes(missingGenotypes);
        variantStats.setMgf(minorGenotypeFrequency);
        variantStats.setMgfGenotype(minorGenotype);
        variantStats.setGenotypesCount(genotypeCount);
        variantStats.setMissingAlleles(missingAlleles);
        variantStats.setMaf(minorAlleleFrequency);
        variantStats.setMafAllele(minorAllele);

        return variantStats;
    }

}
