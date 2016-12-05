package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceEntryConverter implements ComplexTypeConverter<VariantSourceEntry, DBObject> {

    public final static String FILEID_FIELD = "fid";

    public final static String STUDYID_FIELD = "sid";

    public final static String ALTERNATES_FIELD = "alts";

    public final static String ATTRIBUTES_FIELD = "attrs";

    public final static String FORMAT_FIELD = "fm";

    public final static String SAMPLES_FIELD = "samp";

    static final char CHARACTER_TO_REPLACE_DOTS = (char) 163; // <-- £

    private VariantStorageManager.IncludeSrc includeSrc;

    private DBObjectToSamplesConverter samplesConverter;

    /**
     * Create a converter between VariantSourceEntry and DBObject entities when
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc If true, will include and gzip the "src" attribute in the DBObject
     */
    public DBObjectToVariantSourceEntryConverter(VariantStorageManager.IncludeSrc includeSrc) {
        this.includeSrc = includeSrc;
        this.samplesConverter = null;
    }


    /**
     * Create a converter from VariantSourceEntry to DBObject entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DBObjectToVariantSourceEntryConverter(VariantStorageManager.IncludeSrc includeSrc,
                                                 DBObjectToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.samplesConverter = samplesConverter;
    }

    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        throw new UnsupportedOperationException(
                "This version of " + getClass().getSimpleName() + " is only for writing, try the version in OpenCGA");
    }

    @Override
    public DBObject convertToStorageType(VariantSourceEntry object) {
        BasicDBObject mongoFile = new BasicDBObject(FILEID_FIELD, object.getFileId())
                .append(STUDYID_FIELD, object.getStudyId());

        // Alternate alleles
        if (object
                .getSecondaryAlternates().length > 0) {   // assuming secondaryAlternates doesn't contain the primary alternate
            mongoFile.append(ALTERNATES_FIELD, object.getSecondaryAlternates());
        }

        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                Object value = entry.getValue();
                if (entry.getKey().equals("src")) {
                    if (VariantStorageManager.IncludeSrc.FULL.equals(includeSrc)) {
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(entry.getValue());
                        } catch (IOException ex) {
                            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName())
                                  .log(Level.SEVERE, null, ex);
                        }
                    } else if (VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS.equals(includeSrc)) {
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
                    } else {
                        continue;
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

//        if (samples != null && !samples.isEmpty()) {
        if (samplesConverter != null) {
            mongoFile.append(FORMAT_FIELD, object.getFormat()); // Useless field if genotypeCodes are not stored
            mongoFile.put(SAMPLES_FIELD, samplesConverter.convertToStorageType(object));
        }


        return mongoFile;
    }

    public void setIncludeSrc(VariantStorageManager.IncludeSrc includeSrc) {
        this.includeSrc = includeSrc;
    }
}
