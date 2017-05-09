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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter;
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts Variants into MongoDb objects. Implements spring's interface of converter.
 * <p>
 * Design policies:
 * <p>
 * IDS: The ids of a Variant will NOT be put in the DBObject, as using addToSet(ids) and
 * setOnInsert(without ids) avoids overwriting ids. In a DBObject, both an empty ids array or no ids property,
 * converts to a Variant with an empty set of ids.
 * <p>
 * This class is based on OpenCGA MongoDB converters.
 */
public class VariantToDBObjectConverter implements Converter<Variant, DBObject> {

    public static final String ONE_THOUSAND_STRING = VariantMongoDBWriter.CHUNK_SIZE_SMALL / 1000 + "k";

    public static final String TEN_THOUSAND_STRING = VariantMongoDBWriter.CHUNK_SIZE_BIG / 1000 + "k";

    public final static String CHROMOSOME_FIELD = "chr";

    public final static String START_FIELD = "start";

    public final static String END_FIELD = "end";

    public final static String LENGTH_FIELD = "len";

    public final static String REFERENCE_FIELD = "ref";

    public final static String ALTERNATE_FIELD = "alt";

    //    public final static String ID_FIELD = "id";
    public final static String IDS_FIELD = "ids";

    public final static String HGVS_FIELD = "hgvs";

    public final static String TYPE_FIELD = "type";

    public final static String NAME_FIELD = "name";

    public final static String FILES_FIELD = "files";

    public final static String EFFECTS_FIELD = "effs";

    public final static String SOTERM_FIELD = "so";

    public final static String GENE_FIELD = "gene";

    public final static String ANNOTATION_FIELD = "annot";

    public final static String STATS_FIELD = "st";

    private VariantSourceEntryToDBObjectConverter variantSourceEntryConverter;

    private VariantStatsToDBObjectConverter statsConverter;

    /**
     * Create a converter between Variant and DBObject entities when the fields of VariantSourceEntry,
     * Annotation and VariantStats should not be written.
     */
    public VariantToDBObjectConverter() {
        this(null, null);
    }

    /**
     * Create a converter between Variant and DBObject entities. For complex inner fields  (VariantSourceEntry,
     * VariantStats, Annotation), converters must be provided. If they are null, it is assumed that the field
     * should not be written.
     *
     * @param variantSourceEntryConverter Nullable
     * @param VariantStatsConverter       Nullable
     */
    public VariantToDBObjectConverter(
            VariantSourceEntryToDBObjectConverter variantSourceEntryConverter,
            VariantStatsToDBObjectConverter VariantStatsConverter) {
        this.variantSourceEntryConverter = variantSourceEntryConverter;
        this.statsConverter = VariantStatsConverter;
    }

    @Override
    public DBObject convert(Variant object) {
        String id = Variant.buildVariantId(object.getChromosome(), object.getStart(), object.getReference(),
                object.getAlternate());

        BasicDBObject mongoVariant = new BasicDBObject("_id", id)
                // Do not include IDs: the MongoWriter will take care in the query using an $addToSet
                //.append(IDS_FIELD, object.getIds())
                .append(TYPE_FIELD, object.getType().name())
                .append(CHROMOSOME_FIELD, object.getChromosome())
                .append(START_FIELD, object.getStart())
                .append(END_FIELD, object.getEnd())
                .append(LENGTH_FIELD, object.getLength())
                .append(REFERENCE_FIELD, object.getReference())
                .append(ALTERNATE_FIELD, object.getAlternate());

        appendAt(object, mongoVariant);
        appendHgvs(object, mongoVariant);
        appendFiles(object, mongoVariant);
        appendStatistics(object, mongoVariant);

        return mongoVariant;
    }

    /**
     * Internal fields used for query optimization (dictionary named "_at")
     */
    private void appendAt(Variant object, BasicDBObject mongoVariant) {
        BasicDBObject _at = new BasicDBObject();
        _at.append("chunkIds", getChunkIds(object));
        mongoVariant.append("_at", _at);
    }

    /**
     * ChunkIDs (1k and 10k) are a field that all variants within a chunk will share. This is intended for fast access
     * to a specific region using a small index table (compared with and index on chr+start, for instance).
     * This design should be reevaluated if we are using sharded databases.
     */
    private BasicDBList getChunkIds(Variant object) {
        int smallChunkId = object.getStart() / VariantMongoDBWriter.CHUNK_SIZE_SMALL;
        int bigChunkId = object.getStart() / VariantMongoDBWriter.CHUNK_SIZE_BIG;
        String chunkSmall = object.getChromosome() + "_" + smallChunkId + "_" + ONE_THOUSAND_STRING;
        String chunkBig = object.getChromosome() + "_" + bigChunkId + "_" + TEN_THOUSAND_STRING;

        BasicDBList chunkIds = new BasicDBList();
        chunkIds.add(chunkSmall);
        chunkIds.add(chunkBig);
        return chunkIds;
    }

    /**
     * Transform HGVS: Map of sets -> List of map entries
     * {k1 -> {v1_1, v1_2}, k2 -> {v2}} changes to [{k1, v1_1}, {k1, v1_2}, {k2, v2}]
     */
    private void appendHgvs(Variant object, BasicDBObject mongoVariant) {
        BasicDBList hgvs = new BasicDBList();
        for (Map.Entry<String, Set<String>> entry : object.getHgvs().entrySet()) {
            for (String value : entry.getValue()) {
                hgvs.add(new BasicDBObject(TYPE_FIELD, entry.getKey()).append(NAME_FIELD, value));
            }
        }
        mongoVariant.append(HGVS_FIELD, hgvs);
    }

    private void appendFiles(Variant object, BasicDBObject mongoVariant) {
        if (variantSourceEntryConverter != null) {
            BasicDBList mongoFiles = new BasicDBList();
            for (VariantSourceEntry archiveFile : object.getSourceEntries().values()) {
                mongoFiles.add(variantSourceEntryConverter.convert(archiveFile));
            }
            mongoVariant.append(FILES_FIELD, mongoFiles);
        }
    }

    private void appendStatistics(Variant object, BasicDBObject mongoVariant) {
        if (statsConverter != null) {
            List<DBObject> mongoStats = new ArrayList<>();
            for (VariantSourceEntry variantSourceEntry : object.getSourceEntries().values()) {
                mongoStats.addAll(statsConverter.convert(variantSourceEntry));
            }
            mongoVariant.put(STATS_FIELD, mongoStats);
        }
    }
}
