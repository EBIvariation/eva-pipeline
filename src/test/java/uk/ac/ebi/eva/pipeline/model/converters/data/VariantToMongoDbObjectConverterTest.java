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
package uk.ac.ebi.eva.pipeline.model.converters.data;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStats;

import java.util.LinkedHashMap;
import java.util.Map;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

/**
 * Test {@link VariantToMongoDbObjectConverter}
 */
public class VariantToMongoDbObjectConverterTest {
    private GenericConversionService conversionService = new GenericConversionService();
    private Converter variantToMongoDbObjectConverter;

    private final String fileId = "fileId";
    private final String studyId = "studyId";
    private final Map<String, Integer> samplesPosition = new LinkedHashMap<>();
    private final VariantStorageManager.IncludeSrc includeSrc = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

    @Test(expected = IllegalArgumentException.class)
    public void convertNullVariantShouldThrowAnException(){
        variantToMongoDbObjectConverter = new VariantToMongoDbObjectConverter(false, false, true,
                                                                              includeSrc);
        variantToMongoDbObjectConverter.convert(null);
    }

    @Test
    public void allFieldsOfVariantShouldBeConverted(){
        String chromosome = "12";
        int start = 3;
        int end = 4;
        String reference = "A";
        String alternate = "T";

        Variant variant = buildVariant(chromosome, start, end, reference, alternate, fileId, studyId);

        variantToMongoDbObjectConverter = new VariantToMongoDbObjectConverter(false, false, true,
                                                                              includeSrc);
        conversionService.addConverter(variantToMongoDbObjectConverter);

        DBObject dbObject = conversionService.convert(variant, DBObject.class);

        assertNotNull(dbObject);
        BasicDBObject addToSet = (BasicDBObject) dbObject.get("$addToSet");
        BasicDBObject files = (BasicDBObject) addToSet.get("files");
        assertTrue(fileId.equals(files.get("fid")));
        assertTrue(studyId.equals(files.get("sid")));

        BasicDBObject setOnInsert = (BasicDBObject) dbObject.get("$setOnInsert");
        assertTrue(String.format("%s_%s_%s_%s", chromosome, start, reference, alternate).equals(setOnInsert.get("_id")));
        assertTrue(chromosome.equals(setOnInsert.get("chr")));
        assertEquals(start, setOnInsert.get("start"));
        assertEquals(end, setOnInsert.get("end"));
        assertTrue(reference.equals(setOnInsert.get("ref")));
        assertTrue(alternate.equals(setOnInsert.get("alt")));
    }

    @Test
    public void includeStatsTrueShouldIncludeStatistics() {
        Variant variant = buildVariant("12", 3, 4, "A", "T", fileId, studyId);

        variantToMongoDbObjectConverter = new VariantToMongoDbObjectConverter(true,
                                                                              true, false, includeSrc);
        conversionService.addConverter(variantToMongoDbObjectConverter);

        DBObject dbObject = conversionService.convert(variant, DBObject.class);

        assertNotNull(dbObject);
        BasicDBObject addToSet = (BasicDBObject) dbObject.get("$addToSet");
        BasicDBObject st = (BasicDBObject) addToSet.get("st");
        assertNotNull(st);
    }

    @Test
    public void includeStatsFalseShouldNotIncludeStatistics() {
        Variant variant = buildVariant("12", 3, 4, "A", "T", fileId, studyId);

        variantToMongoDbObjectConverter = new VariantToMongoDbObjectConverter(false, false, true,
                                                                              includeSrc);
        conversionService.addConverter(variantToMongoDbObjectConverter);

        DBObject dbObject = conversionService.convert(variant, DBObject.class);

        assertNotNull(dbObject);
        BasicDBObject addToSet = (BasicDBObject) dbObject.get("$addToSet");
        BasicDBObject st = (BasicDBObject) addToSet.get("st");
        assertNull(st);
    }

    @Test
    public void idsIfPresentShouldBeWrittenIntoTheVariant()  {
        Variant variant = buildVariant("12", 3, 4, "A", "T", fileId, studyId);
        variant.setIds(Sets.newHashSet("a", "b", "c"));

        variantToMongoDbObjectConverter = new VariantToMongoDbObjectConverter(false, false, true,
                                                                              includeSrc);
        conversionService.addConverter(variantToMongoDbObjectConverter);

        DBObject dbObject = conversionService.convert(variant, DBObject.class);

        assertNotNull(dbObject);
        BasicDBObject addToSet = (BasicDBObject) dbObject.get("$addToSet");
        BasicDBObject ids = (BasicDBObject) addToSet.get("ids");
        assertNotNull(ids);
    }

    @Test
    public void idsIfNotPresentShouldNotBeWrittenIntoTheVariant()  {
        Variant variant = buildVariant("12", 3, 4, "A", "T", fileId, studyId);

        variantToMongoDbObjectConverter = new VariantToMongoDbObjectConverter(false, false, true,
                                                                              includeSrc);
        conversionService.addConverter(variantToMongoDbObjectConverter);

        DBObject dbObject = conversionService.convert(variant, DBObject.class);

        assertNotNull(dbObject);
        BasicDBObject addToSet = (BasicDBObject) dbObject.get("$addToSet");
        assertNull(addToSet.get("ids"));
    }

    private Variant buildVariant(String chromosome, int start, int end, String reference, String alternate,
                                 String fileId, String studyId){
        Variant variant = new Variant(chromosome, start, end, reference, alternate);

        Map<String, VariantSourceEntry> sourceEntries = new LinkedHashMap<>();
        VariantSourceEntry variantSourceEntry = new VariantSourceEntry(fileId, studyId);
        variantSourceEntry.setCohortStats("cohortStats", new VariantStats(reference, alternate, Variant.VariantType.SNV));
        sourceEntries.put("variant", variantSourceEntry);
        variant.setSourceEntries(sourceEntries);

        return variant;
    }

}
