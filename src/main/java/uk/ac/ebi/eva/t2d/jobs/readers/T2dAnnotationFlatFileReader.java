package uk.ac.ebi.eva.t2d.jobs.readers;

import org.springframework.batch.item.file.FlatFileItemReader;
import uk.ac.ebi.eva.t2d.mapper.T2dAnnotationLineMapper;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class T2dAnnotationFlatFileReader extends FlatFileItemReader<T2dAnnotation> {

    public T2dAnnotationFlatFileReader(String vepOutput) throws IOException {
        setResource(getResource(new File(vepOutput)));
        setLineMapper(new T2dAnnotationLineMapper());
    }

}
