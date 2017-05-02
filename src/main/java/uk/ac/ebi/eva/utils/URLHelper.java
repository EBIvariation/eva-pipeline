/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class URLHelper {

    private static final String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";

    private static final String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";

    public static final String ANNOTATED_VARIANTS_SUFFIX = "_vep_annotation.tsv.gz";

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(input);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }

    public static URI getVariantsStatsUri(String outputDirStatistics, String studyId, String fileId) throws URISyntaxException {
        return URLHelper.createUri(
                getStatsBaseUri(outputDirStatistics, studyId, fileId).getPath() + VARIANT_STATS_SUFFIX);
    }

    public static URI getSourceStatsUri(String outputDirStatistics, String studyId, String fileId) throws URISyntaxException {
        return URLHelper.createUri(
                getStatsBaseUri(outputDirStatistics, studyId, fileId).getPath() + SOURCE_STATS_SUFFIX);
    }

    public static URI getStatsBaseUri(String outputDirStatistics, String studyId, String fileId) throws URISyntaxException {
        URI outdirUri = URLHelper.createUri(outputDirStatistics);
        return outdirUri.resolve(buildSourceEntryId(studyId, fileId));
    }

    public static String buildSourceEntryId(String studyId, String fileId) {
        return studyId + "_" + fileId;
    }

    public static String resolveVepOutput(String outputDirAnnotation, String studyId, String vcfId) {
        return outputDirAnnotation + "/" + studyId + "_" + vcfId + ANNOTATED_VARIANTS_SUFFIX;
    }
}
