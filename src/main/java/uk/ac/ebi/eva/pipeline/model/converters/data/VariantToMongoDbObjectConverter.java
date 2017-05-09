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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.converters.data.SamplesToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantSourceEntryToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantStatsToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.List;

/**
 * Converts a {@link Variant} into mongoDb {@link DBObject}
 */
public class VariantToMongoDbObjectConverter implements Converter<Variant, DBObject> {
    private static final Logger logger = LoggerFactory.getLogger(VariantToMongoDbObjectConverter.class);

    private VariantToDBObjectConverter variantConverter;
    private VariantStatsToDBObjectConverter statsConverter;
    private VariantSourceEntryToDBObjectConverter sourceEntryConverter;

    private boolean includeStats;

    public VariantToMongoDbObjectConverter(boolean includeStats, boolean includeSample) {
        this(includeStats, includeStats, includeSample);
    }

    public VariantToMongoDbObjectConverter(boolean includeStats, boolean calculateStats, boolean includeSample) {

        this.includeStats = includeStats;
        this.statsConverter = calculateStats ? new VariantStatsToDBObjectConverter() : null;


        SamplesToDBObjectConverter sampleConverter = includeSample ? new SamplesToDBObjectConverter() : null;
        this.sourceEntryConverter = new VariantSourceEntryToDBObjectConverter(sampleConverter);
        this.variantConverter = new VariantToDBObjectConverter(null,  null);
    }

    @Override
    public DBObject convert(Variant variant) {
        Assert.notNull(variant, "Variant should not be null. Please provide a valid Variant object");
        logger.trace("Convert variant {} into mongo object", variant);

        VariantSourceEntry variantSourceEntry = variant.getSourceEntries().values().iterator().next();

        BasicDBObject addToSet = new BasicDBObject().append(VariantToDBObjectConverter.FILES_FIELD,
                sourceEntryConverter.convert(variantSourceEntry));

        if (includeStats) {
            List<DBObject> sourceEntryStats = statsConverter.convert(variantSourceEntry);
            addToSet.put(VariantToDBObjectConverter.STATS_FIELD, new BasicDBObject("$each", sourceEntryStats));
        }

        if (variant.getIds() != null && !variant.getIds().isEmpty()) {
            addToSet.put(VariantToDBObjectConverter.IDS_FIELD, new BasicDBObject("$each", variant.getIds()));
        }

        BasicDBObject update = new BasicDBObject();
        update.append("$addToSet", addToSet).append("$setOnInsert", variantConverter.convert(variant));

        return update;
    }
}
