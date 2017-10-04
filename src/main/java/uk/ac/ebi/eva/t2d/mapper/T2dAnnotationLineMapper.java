package uk.ac.ebi.eva.t2d.mapper;

import org.springframework.batch.item.file.LineMapper;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

public class T2dAnnotationLineMapper implements LineMapper<T2dAnnotation> {

    @Override
    public T2dAnnotation mapLine(String line, int i) throws Exception {
        return new T2dAnnotation(line.split("\t"));
    }
}
