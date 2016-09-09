package uk.ac.ebi.eva.pipeline.io.readers;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.io.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
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
