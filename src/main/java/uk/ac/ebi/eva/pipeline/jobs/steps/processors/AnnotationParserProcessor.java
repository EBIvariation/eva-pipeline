/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.pipeline.io.VepProcess;
import uk.ac.ebi.eva.pipeline.io.mappers.AnnotationLineMapper;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ItemStreamWriter that takes VariantWrappers and serialize them into a {@link VepProcess}, which will be responsible
 * for annotating the variants and writing them to a file.
 */
public class AnnotationParserProcessor implements ItemProcessor<List<String>, List<AnnotationMongo>> {

    private static final int UNUSED_LINE_NUMBER = 0;

    private AnnotationLineMapper annotationLineMapper;

    public AnnotationParserProcessor(AnnotationParameters annotationParameters) {
        annotationLineMapper = new AnnotationLineMapper(annotationParameters.getVepVersion(),
                                                        annotationParameters.getVepCacheVersion());
    }

    @Override
    public List<AnnotationMongo> process(List<String> ensemblVariants) throws Exception {
        return ensemblVariants.stream()
                              .map(ensemblVariant -> annotationLineMapper.mapLine(ensemblVariant, UNUSED_LINE_NUMBER))
                              .collect(Collectors.toList());
    }
}
