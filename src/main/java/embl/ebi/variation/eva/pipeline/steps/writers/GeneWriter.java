package embl.ebi.variation.eva.pipeline.steps.writers;

import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;

public class GeneWriter extends MongoItemWriter<FeatureCoordinates> {

    public GeneWriter(ObjectMap pipelineOptions) {
        super();
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions);
        setCollection(pipelineOptions.getString("db.collections.features.name"));
        setTemplate(mongoOperations);
    }
}
