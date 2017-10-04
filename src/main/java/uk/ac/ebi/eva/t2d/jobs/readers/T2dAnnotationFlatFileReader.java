package uk.ac.ebi.eva.t2d.jobs.readers;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;
import uk.ac.ebi.eva.pipeline.io.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper;
import uk.ac.ebi.eva.t2d.mapper.T2dAnnotationLineMapper;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

public class T2dAnnotationFlatFileReader extends FlatFileItemReader<T2dAnnotation> {

    public T2dAnnotationFlatFileReader(String vepOutput) {
        Resource resource = new GzipLazyResource(vepOutput);
        setResource(resource);
        setLineMapper(new T2dAnnotationLineMapper());
    }

}
