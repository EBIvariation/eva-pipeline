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
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Converter of VariantSourceEntry to DBObject. Implements spring's interface of converter.
 * <p>
 * This class is based on OpenCGA MongoDB converters.
 */
public class DBObjectToVariantSourceEntryConverter implements Converter<VariantSourceEntry, DBObject> {

    public final static String FILEID_FIELD = "fid";

    public final static String STUDYID_FIELD = "sid";

    public final static String ALTERNATES_FIELD = "alts";

    public final static String ATTRIBUTES_FIELD = "attrs";

    public final static String FORMAT_FIELD = "fm";

    public final static String SAMPLES_FIELD = "samp";

    static final char CHARACTER_TO_REPLACE_DOTS = (char) 163; // <-- £


    private DBObjectToSamplesConverter samplesConverter;

    /**
     * Create a converter between VariantSourceEntry and DBObject entities when
     * there is no need to provide a list of samples or statistics.
     */
    public DBObjectToVariantSourceEntryConverter() {
        this(null);
    }

    /**
     * Create a converter from VariantSourceEntry to DBObject entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DBObjectToVariantSourceEntryConverter(DBObjectToSamplesConverter samplesConverter) {
        this.samplesConverter = samplesConverter;
    }

    @Override
    public DBObject convert(VariantSourceEntry object) {
        BasicDBObject mongoFile = new BasicDBObject(FILEID_FIELD, object.getFileId())
                .append(STUDYID_FIELD, object.getStudyId());

        // Alternate alleles
        // assuming secondaryAlternates doesn't contain the primary alternate
        if (object.getSecondaryAlternates().length > 0) {
            mongoFile.append(ALTERNATES_FIELD, object.getSecondaryAlternates());
        }

        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                Object value = entry.getValue();
                if (entry.getKey().equals("src")) {
                    String[] fields = entry.getValue().split("\t");
                    StringBuilder sb = new StringBuilder();
                    sb.append(fields[0]);
                    for (int i = 1; i < fields.length && i < 8; i++) {
                        sb.append("\t").append(fields[i]);
                    }
                    try {
                        value = org.opencb.commons.utils.StringUtils.gzip(sb.toString());
                    } catch (IOException ex) {
                        Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName())
                              .log(Level.SEVERE, null, ex);
                    }
                }

                if (attrs == null) {
                    attrs = new BasicDBObject(entry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS), value);
                } else {
                    attrs.append(entry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS), value);
                }
            }

            if (attrs != null) {
                mongoFile.put(ATTRIBUTES_FIELD, attrs);
            }
        }

        if (samplesConverter != null) {
            mongoFile.append(FORMAT_FIELD, object.getFormat()); // Useless field if genotypeCodes are not stored
            mongoFile.put(SAMPLES_FIELD, samplesConverter.convert(object));
        }

        return mongoFile;
    }

}
