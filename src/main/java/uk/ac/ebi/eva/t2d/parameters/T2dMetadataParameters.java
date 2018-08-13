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
import uk.ac.ebi.eva.t2d.entity.DatasetMetadata;
import uk.ac.ebi.eva.t2d.entity.DatasetVersionMetadata;
import uk.ac.ebi.eva.t2d.entity.SamplesDatasetMetadata;

@Service
@JobScope
public class T2dMetadataParameters {

    private static final String PARAMETER = "#{jobParameters['";
    private static final String END = "']}";

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STUDY_TYPE + END)
    private String studyType;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STUDY_GENERATOR + END)
    private String studyGenerator;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STUDY_ANCESTRY + END)
    private String studyAncestry;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STUDY_VERSION + END)
    private String studyVersion;

    @Value(PARAMETER + T2dJobParametersNames.INPUT_STUDY_RELEASE + END)
    private String studyRelease;

    public String getStudyType() {
        return studyType;
    }

    public String getStudyGenerator() {
        return studyGenerator;
    }

    public String getStudyAncestry() {
        return studyAncestry;
    }

    public String getStudyVersion() {
        return studyVersion;
    }

    public String getStudyRelease() {
        return studyRelease;
    }

    public SamplesDatasetMetadata getSamplesMetadata() {
        return new SamplesDatasetMetadata(getStudyGenerator(), getStudyType(),
                Integer.parseInt(getStudyVersion()), getStudyAncestry(),
                Integer.parseInt(getStudyRelease()));
    }


    public DatasetMetadata getDatasetMetadata() {
        return new DatasetMetadata(getStudyGenerator(), getStudyType(),
                Integer.parseInt(getStudyVersion()), getStudyAncestry());
    }

    public DatasetVersionMetadata getDatasetVersionMetadata() {
        return new DatasetVersionMetadata(getStudyGenerator(), getStudyType(),
                Integer.parseInt(getStudyVersion()), Integer.parseInt(getStudyRelease()));
    }
}
