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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.commons.core.models.VariantStatistics;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.genotype.Genotype;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.VariantStatisticsMongo;
import uk.ac.ebi.eva.test.configuration.MongoOperationConfiguration;

import static org.junit.Assert.assertEquals;

/**
 * Tests automatic conversion of {@link VariantStatisticsMongo} to a {@link DBObject}
 */
@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:test-mongo.properties"})
@ContextConfiguration(classes = {MongoOperationConfiguration.class})
public class VariantStatsToDBObjectConverterTest {

    @Autowired
    private MongoOperations mongoOperations;

    private static BasicDBObject mongoStats;

    private static VariantSourceEntry sourceEntry;

    @BeforeClass
    public static void setUpClass() {
        mongoStats = new BasicDBObject(VariantStatisticsMongo.MAF_FIELD, 0.1);
        mongoStats.append(VariantStatisticsMongo.MGF_FIELD, 0.01);
        mongoStats.append(VariantStatisticsMongo.MAFALLELE_FIELD, "A");
        mongoStats.append(VariantStatisticsMongo.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(VariantStatisticsMongo.MISSALLELE_FIELD, 10);
        mongoStats.append(VariantStatisticsMongo.MISSGENOTYPE_FIELD, 5);

        BasicDBObject genotypes = new BasicDBObject();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(VariantStatisticsMongo.NUMGT_FIELD, genotypes);

        VariantStatistics stats = new VariantStatistics(null, null, VariantType.SNV, 0.1f, 0.01f, "A", "A/A",
                10, 5, -1, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);

        sourceEntry = new VariantSourceEntry("f1", "s1");
        sourceEntry.setCohortStats("ALL", stats);
    }


    @Test
    public void testConvertToStorageType() {
        VariantStatistics stats = sourceEntry.getCohortStats("ALL");
        Document converted = (Document) mongoOperations.getConverter().convertToMongoType(
                new VariantStatisticsMongo(
                        sourceEntry.getStudyId(),
                        sourceEntry.getFileId(),
                        "ALL",
                        stats
                )
        );

        //DBObject converted = convertedSourceEntry.get(0);

        assertEquals(stats.getMaf(), (float) converted.get(VariantStatisticsMongo.MAF_FIELD), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.get(VariantStatisticsMongo.MGF_FIELD), 1e-6);
        assertEquals(stats.getMafAllele(), converted.get(VariantStatisticsMongo.MAFALLELE_FIELD));
        assertEquals(stats.getMgfGenotype(), converted.get(VariantStatisticsMongo.MGFGENOTYPE_FIELD));

        assertEquals(stats.getMissingAlleles(), converted.get(VariantStatisticsMongo.MISSALLELE_FIELD));
        assertEquals(stats.getMissingGenotypes(), converted.get(VariantStatisticsMongo.MISSGENOTYPE_FIELD));

        assertEquals(100, ((Document) converted.get(VariantStatisticsMongo.NUMGT_FIELD)).get("0/0"));
        assertEquals(50, ((Document) converted.get(VariantStatisticsMongo.NUMGT_FIELD)).get("0/1"));
        assertEquals(10, ((Document) converted.get(VariantStatisticsMongo.NUMGT_FIELD)).get("1/1"));
    }
}
