package uk.ac.ebi.eva.pipeline.steps.readers;

import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.annotation.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.annotation.load.VariantAnnotationLineMapper;

public class VariantAnnotationReader extends FlatFileItemReader<VariantAnnotation>{

    public VariantAnnotationReader(ObjectMap pipelineOptions) {
        Resource resource = new GzipLazyResource(pipelineOptions.getString("vep.output"));
        setResource(resource);
        setLineMapper(new VariantAnnotationLineMapper());
    }
}
