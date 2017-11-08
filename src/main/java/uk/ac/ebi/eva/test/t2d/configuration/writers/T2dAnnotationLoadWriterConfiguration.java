package uk.ac.ebi.eva.test.t2d.configuration.writers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.t2d.jobs.writers.T2dVepAnnotationFileWriter;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_ANNOTATION_LOAD_WRITER;

@Configuration
public class T2dAnnotationLoadWriterConfiguration {

    @Bean(T2D_ANNOTATION_LOAD_WRITER)
    @StepScope
    public ItemWriter<T2dAnnotation> vepAnnotationFileWriter() {
        return new T2dVepAnnotationFileWriter();
    }

}
