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
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantStatsConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.List;
import java.util.Map;

/**
 * Converts a {@link Variant} into mongoDb {@link DBObject}
 */
public class VariantToMongoDbObjectConverter implements Converter<Variant, DBObject> {
    private static final Logger logger = LoggerFactory.getLogger(VariantToMongoDbObjectConverter.class);

    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceEntryConverter sourceEntryConverter;

    private boolean includeStats;

    public VariantToMongoDbObjectConverter(boolean includeStats, Map<String, Integer> samplesPosition,
                                           boolean calculateStats, boolean includeSample,
                                           VariantStorageManager.IncludeSrc includeSrc) {
        this.includeStats = includeStats;
        this.statsConverter = calculateStats ? new DBObjectToVariantStatsConverter() : null;
        DBObjectToSamplesConverter sampleConverter =
                includeSample ? new DBObjectToSamplesConverter(true, samplesPosition) : null;
        this.sourceEntryConverter = new DBObjectToVariantSourceEntryConverter(includeSrc, sampleConverter);
        this.variantConverter = new DBObjectToVariantConverter(null, null);
    }

    @Override
    public DBObject convert(Variant variant) {
        Assert.notNull(variant, "Variant should not be null. Please provide a valid Variant object");
        logger.trace("Convert variant {} into mongo object", variant);

        variant.setAnnotation(null);

        VariantSourceEntry variantSourceEntry = variant.getSourceEntries().entrySet().iterator().next().getValue();

        BasicDBObject addToSet = new BasicDBObject().append(DBObjectToVariantConverter.FILES_FIELD,
                        sourceEntryConverter.convertToStorageType(variantSourceEntry));

        if (includeStats) {
            List<DBObject> sourceEntryStats =
                    statsConverter.convertCohortsToStorageType(variantSourceEntry.getCohortStats(),
                    variantSourceEntry.getStudyId(), variantSourceEntry.getFileId());
            addToSet.put(DBObjectToVariantConverter.STATS_FIELD, new BasicDBObject("$each", sourceEntryStats));
        }

        if (variant.getIds() != null && !variant.getIds().isEmpty()) {
            addToSet.put(DBObjectToVariantConverter.IDS_FIELD, new BasicDBObject("$each", variant.getIds()));
        }

        BasicDBObject update = new BasicDBObject();
        update.append("$addToSet", addToSet).append("$setOnInsert", variantConverter.convertToStorageType(variant));

        return update;
    }
}
