package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link VariantStatsToDBObjectConverter}
 * <p>
 * Input: {@link VariantSourceEntry}
 * output: DBObject representing the {@link VariantStats} inside the {@link VariantSourceEntry}
 */
public class VariantStatsToDBObjectConverterTest {

    private static BasicDBObject mongoStats;

    private static VariantSourceEntry sourceEntry;

    @BeforeClass
    public static void setUpClass() {
        mongoStats = new BasicDBObject(VariantStatsToDBObjectConverter.MAF_FIELD, 0.1);
        mongoStats.append(VariantStatsToDBObjectConverter.MGF_FIELD, 0.01);
        mongoStats.append(VariantStatsToDBObjectConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(VariantStatsToDBObjectConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(VariantStatsToDBObjectConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(VariantStatsToDBObjectConverter.MISSGENOTYPE_FIELD, 5);

        BasicDBObject genotypes = new BasicDBObject();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(VariantStatsToDBObjectConverter.NUMGT_FIELD, genotypes);

        VariantStats stats = new VariantStats(null, -1, null, null, Variant.VariantType.SNV, 0.1f, 0.01f, "A", "A/A",
                                              10, 5, -1, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);

        sourceEntry = new VariantSourceEntry("f1", "s1");
        sourceEntry.setCohortStats("ALL", stats);
    }


    @Test
    public void testConvertToStorageType() {
        VariantStatsToDBObjectConverter converter = new VariantStatsToDBObjectConverter();
        List<DBObject> convertedSourceEntry = converter.convert(sourceEntry);
        assertEquals(1, convertedSourceEntry.size());

        DBObject converted = convertedSourceEntry.get(0);
        VariantStats stats = sourceEntry.getCohortStats("ALL");

        assertEquals(stats.getMaf(), (float) converted.get(VariantStatsToDBObjectConverter.MAF_FIELD), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.get(VariantStatsToDBObjectConverter.MGF_FIELD), 1e-6);
        assertEquals(stats.getMafAllele(), converted.get(VariantStatsToDBObjectConverter.MAFALLELE_FIELD));
        assertEquals(stats.getMgfGenotype(), converted.get(VariantStatsToDBObjectConverter.MGFGENOTYPE_FIELD));

        assertEquals(stats.getMissingAlleles(), converted.get(VariantStatsToDBObjectConverter.MISSALLELE_FIELD));
        assertEquals(stats.getMissingGenotypes(), converted.get(VariantStatsToDBObjectConverter.MISSGENOTYPE_FIELD));

        assertEquals(100, ((DBObject) converted.get(VariantStatsToDBObjectConverter.NUMGT_FIELD)).get("0/0"));
        assertEquals(50, ((DBObject) converted.get(VariantStatsToDBObjectConverter.NUMGT_FIELD)).get("0/1"));
        assertEquals(10, ((DBObject) converted.get(VariantStatsToDBObjectConverter.NUMGT_FIELD)).get("1/1"));
    }
}
