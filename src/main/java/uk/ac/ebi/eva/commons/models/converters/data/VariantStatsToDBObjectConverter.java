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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.feature.Genotype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Converts VariantStats into MongoDb objects. Implements spring's interface of converter.
 * <p>
 * This class is based on OpenCGA MongoDB converters.
 */
public class VariantStatsToDBObjectConverter implements Converter<VariantSourceEntry, List<DBObject>> {

    public final static String COHORT_ID = "cid";

    public final static String STUDY_ID = "sid";

    public final static String FILE_ID = "fid";

    public final static String MAF_FIELD = "maf";

    public final static String MGF_FIELD = "mgf";

    public final static String MAFALLELE_FIELD = "mafAl";

    public final static String MGFGENOTYPE_FIELD = "mgfGt";

    public final static String MISSALLELE_FIELD = "missAl";

    public final static String MISSGENOTYPE_FIELD = "missGt";

    public final static String NUMGT_FIELD = "numGt";

    protected static Logger logger = LoggerFactory.getLogger(VariantStatsToDBObjectConverter.class);

    /**
     * converts all the cohortstats within the sourceEntries
     *
     * @param sourceEntry for instance, you can pass in variant.getSourceEntries()
     * @return list of VariantStats (as DBObjects)
     */
    @Override
    public List<DBObject> convert(VariantSourceEntry sourceEntry) {
        return convertCohorts(sourceEntry.getCohortStats(), sourceEntry.getStudyId(), sourceEntry.getFileId());
    }

    /**
     * converts just some cohorts stats in one VariantSourceEntry.
     *
     * @param cohortStats for instance, you can pass in sourceEntry.getCohortStats()
     * @param studyId     of the source entry
     * @param fileId      of the source entry
     * @return list of VariantStats (as DBObjects)
     */
    public List<DBObject> convertCohorts(Map<String, VariantStats> cohortStats, String studyId, String fileId) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        VariantStats variantStats;
        for (Map.Entry<String, VariantStats> variantStatsEntry : cohortStats.entrySet()) {
            variantStats = variantStatsEntry.getValue();
            DBObject variantStatsDBObject = convertStats(variantStats);
            variantStatsDBObject.put(VariantStatsToDBObjectConverter.COHORT_ID, variantStatsEntry.getKey());
            variantStatsDBObject.put(VariantStatsToDBObjectConverter.STUDY_ID, studyId);
            variantStatsDBObject.put(VariantStatsToDBObjectConverter.FILE_ID, fileId);
            cohortsStatsList.add(variantStatsDBObject);
        }
        return cohortsStatsList;
    }

    public DBObject convertStats(VariantStats variantStats) {

        // Basic fields
        BasicDBObject mongoStats = new BasicDBObject(MAF_FIELD, variantStats.getMaf());
        mongoStats.append(MGF_FIELD, variantStats.getMgf());
        mongoStats.append(MAFALLELE_FIELD, variantStats.getMafAllele());
        mongoStats.append(MGFGENOTYPE_FIELD, variantStats.getMgfGenotype());
        mongoStats.append(MISSALLELE_FIELD, variantStats.getMissingAlleles());
        mongoStats.append(MISSGENOTYPE_FIELD, variantStats.getMissingGenotypes());

        // Genotype counts
        BasicDBObject genotypes = new BasicDBObject();
        for (Map.Entry<Genotype, Integer> g : variantStats.getGenotypesCount().entrySet()) {
            String genotypeStr = g.getKey().toString().replace(".", "-1");
            genotypes.append(genotypeStr, g.getValue());
        }
        mongoStats.append(NUMGT_FIELD, genotypes);
        return mongoStats;
    }
}

