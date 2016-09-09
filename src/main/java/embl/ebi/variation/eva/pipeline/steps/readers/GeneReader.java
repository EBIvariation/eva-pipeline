package embl.ebi.variation.eva.pipeline.steps.readers;

import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.gene.GeneLineMapper;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import java.io.File;

public class GeneReader extends FlatFileItemReader<FeatureCoordinates> {

    public GeneReader(File file){
        super();
        Resource resource = new GzipLazyResource(file);
        setResource(resource);
        setLineMapper(new GeneLineMapper());
        setComments(new String[] { "#" });   // explicit statement not necessary, it's set up this way by default
    }

    public GeneReader(String filePath){
        this(new File(filePath));
    }

}
