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

package uk.ac.ebi.eva.test.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import static org.junit.Assert.assertEquals;

import static uk.ac.ebi.eva.commons.models.data.VariantSourceEntity.STUDYID_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.FILES_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.STATS_FIELD;

public class DropStudyJobTestUtils {

    private static final String FILES_STUDY_ID_FIELD = String.format("%s.%s", FILES_FIELD, STUDYID_FIELD);

    private static final String STATS_STUDY_ID_FIELD = String.format("%s.%s", STATS_FIELD, STUDYID_FIELD);

    public static void assertDropVariantsByStudy(DBCollection variantsCollection, String studyId,
                                                 long expectedVariantsAfterDropStudy) {

        assertEquals(expectedVariantsAfterDropStudy, variantsCollection.count());

        BasicDBObject singleStudyVariants = new BasicDBObject(FILES_STUDY_ID_FIELD, studyId)
                .append(FILES_FIELD, new BasicDBObject("$size", 1));
        assertEquals(0, variantsCollection.count(singleStudyVariants));
    }

    public static void assertPullStudy(DBCollection variantsCollection, String studyId, long expectedFileCount,
            long expectedStatsCount) {
        BasicDBObject variantFiles = new BasicDBObject(FILES_STUDY_ID_FIELD, studyId);
        BasicDBObject variantStats = new BasicDBObject(STATS_STUDY_ID_FIELD, studyId);

        assertEquals(expectedFileCount, variantsCollection.count(variantFiles));
        assertEquals(expectedStatsCount, variantsCollection.count(variantStats));
    }

    public static void assertDropFiles(DBCollection filesCollection, String studyId, long expectedFilesAfterDropStudy) {
        assertEquals(expectedFilesAfterDropStudy, filesCollection.count());

        BasicDBObject remainingFilesThatShouldHaveBeenDropped = new BasicDBObject(STUDYID_FIELD, studyId);
        assertEquals(0, filesCollection.count(remainingFilesThatShouldHaveBeenDropped));
    }

}
