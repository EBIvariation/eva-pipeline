package uk.ac.ebi.eva.pipeline.io.writers;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.utils.MongoDBHelper;

public class GeneWriter extends MongoItemWriter<FeatureCoordinates> {

    public GeneWriter(MongoOperations mongoOperations, String collectionName) {
        super();
        setCollection(collectionName);
        setTemplate(mongoOperations);
    }
}
