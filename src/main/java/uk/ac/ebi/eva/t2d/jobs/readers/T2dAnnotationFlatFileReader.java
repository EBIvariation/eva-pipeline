/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.jobs.readers;

import org.springframework.batch.item.file.FlatFileItemReader;
import uk.ac.ebi.eva.t2d.mapper.T2dAnnotationLineMapper;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Flat file reader adapted to get VEP output files
 */
public class T2dAnnotationFlatFileReader extends FlatFileItemReader<T2dAnnotation> {

    public T2dAnnotationFlatFileReader(String vepOutput) throws IOException {
        setResource(getResource(new File(vepOutput)));
        setLineMapper(new T2dAnnotationLineMapper());
    }

}
