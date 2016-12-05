package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.datastore.core.ComplexTypeConverter;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simplified samples converter that does not require the names of the samples, as it will compress them in mongo.
 */
public class DBObjectToSamplesConverter implements ComplexTypeConverter<VariantSourceEntry, DBObject> {

    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        throw new UnsupportedOperationException(
                "This version of " + getClass().getSimpleName() + " is only for writing, try the version in OpenCGA");
    }

    @Override
    public DBObject convertToStorageType(VariantSourceEntry object) {
        Map<Genotype, List<Integer>> genotypeCodes = new HashMap<>();

        // Classify samples by genotype
        for (int i = 0; i < object.getSamplesData().size(); i++) {
            String genotype = object.getSampleData(i).get("GT");
            if (genotype != null) {
                Genotype g = new Genotype(genotype);
                List<Integer> samplesWithGenotype = genotypeCodes.get(g);
                if (samplesWithGenotype == null) {
                    samplesWithGenotype = new ArrayList<>();
                    genotypeCodes.put(g, samplesWithGenotype);
                }
                samplesWithGenotype.add(i);
            }
        }

        // Get the most common genotype
        Map.Entry<Genotype, List<Integer>> longestList = null;
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            List<Integer> genotypeList = entry.getValue();
            if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                longestList = entry;
            }
        }

        // In Mongo, samples are stored in a map, classified by their genotype.
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 0|0,
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "1|0" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        BasicDBObject mongoSamples = new BasicDBObject();
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            String genotypeStr = entry.getKey().toString().replace(".", "-1");
            if (longestList != null && entry.getKey().equals(longestList.getKey())) {
                mongoSamples.append("def", genotypeStr);
            } else {
                mongoSamples.append(genotypeStr, entry.getValue());
            }
        }

        return mongoSamples;
    }
}
