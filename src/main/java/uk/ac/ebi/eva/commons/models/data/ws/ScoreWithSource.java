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
package uk.ac.ebi.eva.commons.models.data.ws;

import uk.ac.ebi.eva.commons.models.data.IScore;
import uk.ac.ebi.eva.commons.models.data.Score;

/**
 * Subclass of Score, used in proteinSubstitutionScores in ConsequenceType so webservice returns an array of scores
 * rather than each score at root of consequence type object.
 */
public class ScoreWithSource extends Score
{

    private String source;

    ScoreWithSource() {
        this(null, null);
    }

    public ScoreWithSource(Double score, String description) {
        this(score, description, null);
    }

    public ScoreWithSource(IScore score) {
        this(score.getScore(), score.getDescription());
    }

    public ScoreWithSource(Double score, String description, String source) {
        super(score, description);
        this.source = source;
    }

    public String getSource() {
        return source;
    }
}
