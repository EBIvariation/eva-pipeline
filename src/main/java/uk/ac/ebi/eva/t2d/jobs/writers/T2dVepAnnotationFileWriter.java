package uk.ac.ebi.eva.t2d.jobs.writers;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;
import uk.ac.ebi.eva.t2d.services.T2dService;

import java.util.List;

public class T2dVepAnnotationFileWriter implements ItemWriter<T2dAnnotation> {

    @Autowired
    private T2dService service;

    @Override
    public void write(List<? extends T2dAnnotation> list) throws Exception {
        service.saveAnnotations(list);
    }
}
