package embl.ebi.variation.eva.pipeline.steps.readers;

import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationLineMapper;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

public class VariantAnnotationReader extends FlatFileItemReader<VariantAnnotation>{

    public VariantAnnotationReader(ObjectMap pipelineOptions) {
        Resource resource = new GzipLazyResource(pipelineOptions.getString("vep.output"));
        setResource(resource);
        setLineMapper(new VariantAnnotationLineMapper());
    }
}
