package uk.ac.ebi.eva.t2d.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.t2d.jobs.readers.T2dAnnotationFlatFileReader;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;

import java.io.IOException;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_VARIANT_ANNOTATION_READER;

@Configuration
public class T2dVariantAnnotationReaderConfiguration {

    @Bean(T2D_VARIANT_ANNOTATION_READER)
    @StepScope
    public ItemStreamReader<T2dAnnotation> annotationReader(AnnotationParameters annotationParameters,
                                                            T2dTsvParameters t2dTsvParameters) throws IOException {
        String filename = t2dTsvParameters.getManualVepFile();
        if(filename==null || filename.isEmpty()){
            filename = annotationParameters.getVepOutput();
        }
        return new T2dAnnotationFlatFileReader(filename);
    }

}
