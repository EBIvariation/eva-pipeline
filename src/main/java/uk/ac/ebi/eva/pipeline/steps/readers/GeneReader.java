package uk.ac.ebi.eva.pipeline.steps.readers;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.annotation.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.gene.FeatureCoordinates;
import uk.ac.ebi.eva.pipeline.gene.GeneLineMapper;

public class GeneReader extends FlatFileItemReader<FeatureCoordinates> {

    public GeneReader(ObjectMap pipelineOptions){
        super();
        Resource resource = new GzipLazyResource(pipelineOptions.getString("input.gtf"));
        setResource(resource);
        setLineMapper(new GeneLineMapper());
        setComments(new String[] { "#" });   // explicit statement not necessary, it's set up this way by default
    }

}
