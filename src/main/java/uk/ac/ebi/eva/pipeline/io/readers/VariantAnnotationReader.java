package uk.ac.ebi.eva.pipeline.io.readers;

import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.io.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.io.mappers.VariantAnnotationLineMapper;
import java.io.File;

public class VariantAnnotationReader extends FlatFileItemReader<VariantAnnotation>{

    public VariantAnnotationReader(File file) {
        Resource resource = new GzipLazyResource(file);
        setResource(resource);
        setLineMapper(new VariantAnnotationLineMapper());
    }

    public VariantAnnotationReader(String string) {
        this(new File(string));
    }
}
