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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests {@link VariantToDBObjectConverter}
 * <p>
 * Input: {@link Variant}
 * output: DBObject representing the {@link Variant}
 */
public class VariantToDBObjectConverterTest {

    private BasicDBObject mongoVariant;

    private Variant variant;

    private VariantSourceEntry variantSourceEntry;

    @Before
    public void setUp() {
        //Setup variant
        variant = new Variant("1", 1000, 1000, "A", "C");
        variant.setIds(Collections.singleton("rs666"));
        //Setup variantSourceEntry
        variantSourceEntry = new VariantSourceEntry("f1", "s1");
        variantSourceEntry.addAttribute("QUAL", "0.01");
        variantSourceEntry.addAttribute("AN", "2");
        variantSourceEntry.setFormat("GT:DP");

        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        na001.put("DP", "4");
        variantSourceEntry.addSampleData(na001);
        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        na002.put("DP", "5");
        variantSourceEntry.addSampleData(na002);
        variant.addSourceEntry(variantSourceEntry);

        //Setup mongoVariant
        mongoVariant = new BasicDBObject("_id", "1_1000_A_C")
                .append(VariantToDBObjectConverter.IDS_FIELD, variant.getIds())
                .append(VariantToDBObjectConverter.TYPE_FIELD, variant.getType().name())
                .append(VariantToDBObjectConverter.CHROMOSOME_FIELD, variant.getChromosome())
                .append(VariantToDBObjectConverter.START_FIELD, variant.getStart())
                .append(VariantToDBObjectConverter.END_FIELD, variant.getStart())
                .append(VariantToDBObjectConverter.LENGTH_FIELD, variant.getLength())
                .append(VariantToDBObjectConverter.REFERENCE_FIELD, variant.getReference())
                .append(VariantToDBObjectConverter.ALTERNATE_FIELD, variant.getAlternate());

        BasicDBList chunkIds = new BasicDBList();
        chunkIds.add("1_1_1k");
        chunkIds.add("1_0_10k");
        mongoVariant.append("_at", new BasicDBObject("chunkIds", chunkIds));

        BasicDBList hgvs = new BasicDBList();
        hgvs.add(new BasicDBObject("type", "genomic").append("name", "1:g.1000A>C"));
        mongoVariant.append("hgvs", hgvs);
    }

    @Test
    public void testConvertToStorageTypeWithFiles() {

        variant.addSourceEntry(variantSourceEntry);

        // MongoDB object
        BasicDBObject mongoFile = new BasicDBObject(VariantSourceEntryToDBObjectConverter.FILEID_FIELD,
                                                    variantSourceEntry.getFileId())
                .append(VariantSourceEntryToDBObjectConverter.STUDYID_FIELD, variantSourceEntry.getStudyId())
                .append(VariantSourceEntryToDBObjectConverter.ATTRIBUTES_FIELD,
                        new BasicDBObject("QUAL", "0.01").append("AN", "2"))
                .append(VariantSourceEntryToDBObjectConverter.FORMAT_FIELD, variantSourceEntry.getFormat());

        BasicDBObject genotypeCodes = new BasicDBObject();
        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Collections.singletonList(1));
        mongoFile.append(VariantSourceEntryToDBObjectConverter.SAMPLES_FIELD, genotypeCodes);
        BasicDBList files = new BasicDBList();
        files.add(mongoFile);
        mongoVariant.append("files", files);

        VariantToDBObjectConverter converter = new VariantToDBObjectConverter(
                new VariantSourceEntryToDBObjectConverter(new SamplesToDBObjectConverter()), null);
        DBObject converted = converter.convert(variant);
        assertFalse(converted.containsField(VariantToDBObjectConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(VariantToDBObjectConverter.IDS_FIELD, variant.getIds());  //Add IDs
        assertEquals(mongoVariant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutFiles() {
        VariantToDBObjectConverter converter = new VariantToDBObjectConverter();
        DBObject converted = converter.convert(variant);
        assertFalse(converted.containsField(VariantToDBObjectConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(VariantToDBObjectConverter.IDS_FIELD, variant.getIds());  //Add IDs
        assertEquals(mongoVariant, converted);
    }

    /**
     * @see VariantToDBObjectConverter ids policy
     */
    @Test
    public void testConvertToStorageTypeNullIds() {
        variant.setIds(null);

        VariantToDBObjectConverter converter = new VariantToDBObjectConverter();
        DBObject converted = converter.convert(variant);
        assertFalse(converted.containsField(VariantToDBObjectConverter.IDS_FIELD)); //IDs must be added manually.

        mongoVariant.remove(VariantToDBObjectConverter.IDS_FIELD);
        assertEquals(mongoVariant, converted);
    }

    /**
     * @see VariantToDBObjectConverter ids policy
     */
    @Test
    public void testConvertToStorageTypeEmptyIds() {
        variant.setIds(new HashSet<>());

        VariantToDBObjectConverter converter = new VariantToDBObjectConverter();
        DBObject converted = converter.convert(variant);
        assertFalse(converted.containsField(VariantToDBObjectConverter.IDS_FIELD)); //IDs must be added manually.

        mongoVariant.remove(VariantToDBObjectConverter.IDS_FIELD);
        assertEquals(mongoVariant, converted);
    }

}
