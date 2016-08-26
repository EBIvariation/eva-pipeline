package embl.ebi.variation.eva.pipeline.steps.readers;

import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneLineMapper;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

public class GeneReader extends FlatFileItemReader<FeatureCoordinates> {

    public GeneReader(ObjectMap pipelineOptions){
        super();
        Resource resource = new GzipLazyResource(pipelineOptions.getString("input.gtf"));
        setResource(resource);
        setLineMapper(new GeneLineMapper());
        setComments(new String[] { "#" });   // explicit statement not necessary, it's set up this way by default
    }

}
