package uk.ac.ebi.eva.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ParameterFromProperties {

    private static final Logger logger = LoggerFactory.getLogger(ParameterFromProperties.class);
    private static final String PROPERTY = "${";
    private static final String OR_NULL = ":#{null}}";
    private static final String PROPERTY_ID_REGEX = "(?<=\\$\\{).*(?=:#\\{null})";

    @Value(PROPERTY + JobParametersNames.DB_NAME + OR_NULL)
    private String databaseName;

    @Value(PROPERTY + JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME + OR_NULL)
    private String collectionVariantsName;

    @Value(PROPERTY + JobParametersNames.INPUT_STUDY_ID + OR_NULL)
    private String studyId;

    @Value(PROPERTY + JobParametersNames.INPUT_STUDY_NAME + OR_NULL)
    private String studyName;

    @Value(PROPERTY + JobParametersNames.INPUT_STUDY_TYPE + OR_NULL)
    private String studyType;

    @Value(PROPERTY + JobParametersNames.INPUT_VCF_ID + OR_NULL)
    private String vcfId;

    @Value(PROPERTY + JobParametersNames.INPUT_VCF + OR_NULL)
    private String vcf;

    @Value(PROPERTY + JobParametersNames.INPUT_VCF_AGGREGATION + OR_NULL)
    private String vcfAggregation;

    public Properties getProperties() {
        Properties properties = new Properties();

        for (Field field : getClass().getDeclaredFields()) {
            try {
                if (field.isAnnotationPresent(Value.class)) {
                    Value value = field.getAnnotation(Value.class);
                    String fieldName = value.value();
                    Matcher matcher = Pattern.compile(PROPERTY_ID_REGEX).matcher(fieldName);
                    matcher.find();
                    String propertyName = matcher.group(0);
                    if (field.get(this) != null) {
                        properties.put(propertyName, field.get(this));
                    }
                }
            } catch (IllegalAccessException e) {
                logger.debug("Value retrieval error", e);
            }
        }
        return properties;
    }

}
