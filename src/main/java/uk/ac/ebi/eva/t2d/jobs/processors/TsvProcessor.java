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
package uk.ac.ebi.eva.t2d.jobs.processors;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.model.T2dColumnDefinition;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;

/**
 * Item procesor to transform the raw data form TSV files into the database form
 */
public class TsvProcessor implements ItemProcessor<Map<String, String>, List<String>> {

    private LinkedHashSet<String> fieldNames;
    private Collection<T2dColumnDefinition> definitions;


    @Override
    public List<String> process(Map<String, String> columnIdToValue) throws Exception {
        return definitions.stream()
                .map(definition -> definition.getAdaptor().evaluate(columnIdToValue))
                .collect(Collectors.toList());
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        // This data comes from another step, data is in the job context
        final ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        T2DTableStructure tableStructure = (T2DTableStructure) executionContext.get(CONTEXT_TSV_DEFINITION);
        Assert.notNull(tableStructure, "Could not get table structure from job context");
        definitions = tableStructure.getOrderedDefinitions();
    }
}
