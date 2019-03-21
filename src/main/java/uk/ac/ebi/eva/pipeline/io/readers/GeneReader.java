/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.io.readers;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.io.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;

import java.io.File;

/**
 * Reads a compressed file using a {@link GeneLineMapper}
 */
public class GeneReader extends FlatFileItemReader<FeatureCoordinates> {

    public GeneReader(File file) {
        super();
        Resource resource = new GzipLazyResource(file);
        setResource(resource);
        setLineMapper(new GeneLineMapper());
        setComments(new String[]{"#"});   // explicit statement not necessary, it's set up this way by default
    }

    public GeneReader(String filePath) {
        this(new File(filePath));
    }

}
