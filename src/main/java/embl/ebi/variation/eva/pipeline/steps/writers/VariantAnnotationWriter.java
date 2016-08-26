package embl.ebi.variation.eva.pipeline.steps.writers;

import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationMongoItemWriter;
import org.opencb.datastore.core.ObjectMap;

/**
 * TODO this class is the translation of the original code. This class can be removed lated if parent class can be refactored.
 */
public class VariantAnnotationWriter extends VariantAnnotationMongoItemWriter {

    public VariantAnnotationWriter(ObjectMap pipelineOptions){
        super(MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions));
        setCollection(pipelineOptions.getString("db.collections.variants.name"));
        setTemplate(MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions));
    }
}
