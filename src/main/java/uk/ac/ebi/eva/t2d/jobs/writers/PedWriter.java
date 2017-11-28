/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.t2d.jobs.writers;

import com.google.common.base.Strings;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;

/**
 * Ped Writer, extends a {@link FlatFileItemWriter} and adds a header with the column names and adds a column IID
 * needed by GAIT application.
 */
public class PedWriter extends FlatFileItemWriter<List<String>> {

    private T2DTableStructure tableStructure;

    private int idPosition;

    public PedWriter() {
        setLineAggregator(item -> {
            StringBuilder sb = new StringBuilder();
            sb.append(item.get(idPosition)).append("\t");
            sb.append(item.stream().collect(Collectors.joining("\t")));
            return sb.toString();
        });
        setHeaderCallback(writer -> {
            writer.append("#IID\t");
            writer.append(tableStructure.getOrderedFieldIdSet().stream()
                    .map(value -> {
                        if(Strings.isNullOrEmpty(value)){
                            return "NA";
                        }else{
                            return value;
                        }
                    })
                    .collect(Collectors.joining("\t")));
        });
    }

    @BeforeStep
    public void retrieveSharedStepDate(StepExecution stepExecution) {
        // This data comes from another step, data is in the job context
        JobExecution jobExecution = stepExecution.getJobExecution();
        tableStructure = (T2DTableStructure) jobExecution.getExecutionContext().get(CONTEXT_TSV_DEFINITION);
        Assert.notNull(tableStructure, "Could not get table structure from job context");
        idPosition = new ArrayList<>(tableStructure.getOrderedFieldIdSet()).indexOf("ID");
    }

}
