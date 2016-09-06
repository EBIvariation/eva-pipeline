package embl.ebi.variation.eva.pipeline.steps.readers;

import com.mongodb.DBObject;
import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.data.MongoItemReader;

public class VariantReader extends MongoItemReader<DBObject> {

    public VariantReader(ObjectMap pipelineOptions) {
        setCollection(pipelineOptions.getString("db.collections.variants.name"));

        setQuery("{ \"annot.ct.so\" : { $exists : false } }");
        setFields("{ chr : 1, start : 1, end : 1, ref : 1, alt : 1 }");
        
        setTargetType(DBObject.class);
        setTemplate(MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions));
    }

}
