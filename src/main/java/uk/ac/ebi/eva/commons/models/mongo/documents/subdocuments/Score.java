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

package uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * From {@link org.opencb.biodata.models.variant.annotation.Score}
 */
@Document
public class Score {

    public final static String SCORE_SCORE_FIELD = "sc";

    public final static String SCORE_DESCRIPTION_FIELD = "desc";

    @Field(value = SCORE_SCORE_FIELD)
    private Double score;

    @Field(value = SCORE_DESCRIPTION_FIELD)
    private String description;

    public Score(Double score, String description) {
        this.score = score;
        this.description = description;
    }

    public Double getScore() {
        return score;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Score score1 = (Score) o;

        if (score != null ? !score.equals(score1.score) : score1.score != null) return false;
        return description != null ? description.equals(score1.description) : score1.description == null;
    }

    @Override
    public int hashCode() {
        int result = score != null ? score.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }
}
