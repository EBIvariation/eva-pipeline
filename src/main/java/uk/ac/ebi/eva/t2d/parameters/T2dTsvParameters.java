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
package uk.ac.ebi.eva.t2d.parameters;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.ac.ebi.eva.t2d.entity.Phenotype;

@Service
@JobScope
public class T2dTsvParameters {

    private static final String PARAMETER = "#{jobParameters['";
    private static final String END = "']}";

    @Value(PARAMETER + T2dJobParametersNames.INPUT_SAMPLES + END)
    private String samplesFile;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_SAMPLES_DEFINITION + END)
    private String samplesDefinitionFile;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STATISTICS + END)
    private String summaryStatisticsFile;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STATISTICS_DEFINITION + END)
    private String summaryStatisticsDefinitionFile;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STATISTICS_PHENOTYPE + END)
    private String summaryStatisticsPhenotype;

    @Value(PARAMETER + T2dJobParametersNames.MANUAL_VEP_FILE + END)
    private String manualVepFile;

    public String getSummaryStatisticsFile() {
        return summaryStatisticsFile;
    }

    public String getSummaryStatisticsDefinitionFile() {
        return summaryStatisticsDefinitionFile;
    }

    public Phenotype getSummaryStatisticsPhenotype() {
        if (summaryStatisticsPhenotype == null) {
            return null;
        }
        return new Phenotype(summaryStatisticsPhenotype);
    }

    public String getSamplesFile() {
        return samplesFile;
    }

    public String getSamplesDefinitionFile() {
        return samplesDefinitionFile;
    }

    public String getManualVepFile() {
        return manualVepFile;
    }
}
