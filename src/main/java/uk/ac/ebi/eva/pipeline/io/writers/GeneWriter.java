package uk.ac.ebi.eva.pipeline.io.writers;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.pipeline.MongoDBHelper;
import uk.ac.ebi.eva.pipeline.gene.FeatureCoordinates;

public class GeneWriter extends MongoItemWriter<FeatureCoordinates> {

    public GeneWriter(ObjectMap pipelineOptions) {
        super();
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions);
        setCollection(pipelineOptions.getString("db.collections.features.name"));
        setTemplate(mongoOperations);
    }
}
