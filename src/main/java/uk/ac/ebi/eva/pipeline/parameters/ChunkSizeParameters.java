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
package uk.ac.ebi.eva.pipeline.parameters;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@StepScope
public class ChunkSizeParameters {

    private static final String PARAMETER = "#{jobParameters['";
    private static final String OR_DEFAULT = "']?:'1000'}";

    @Value(PARAMETER + JobParametersNames.CONFIG_CHUNK_SIZE + OR_DEFAULT)
    private Integer chunkSize;

    public Integer getChunkSize() {
        return chunkSize;
    }
}
