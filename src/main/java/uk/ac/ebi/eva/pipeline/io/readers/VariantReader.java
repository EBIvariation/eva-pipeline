package uk.ac.ebi.eva.pipeline.io.readers;

import com.mongodb.DBObject;

import uk.ac.ebi.eva.utils.MongoDBHelper;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.data.domain.Sort;

import java.util.HashMap;
import java.util.Map;


public class VariantReader extends MongoItemReader<DBObject> {

    public VariantReader(ObjectMap pipelineOptions){
        super();

        setCollection(pipelineOptions.getString("db.collections.variants.name"));

        setQuery("{ annot : { $exists : false } }");
        setFields("{ chr : 1, start : 1, end : 1, ref : 1, alt : 1, type : 1}");
        setTargetType(DBObject.class);
        setTemplate(MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions));

        Map<String, Sort.Direction> coordinatesSort = new HashMap<>();
        coordinatesSort.put("chr", Sort.Direction.ASC);
        coordinatesSort.put("start", Sort.Direction.ASC);
        setSort(coordinatesSort);
    }

}
