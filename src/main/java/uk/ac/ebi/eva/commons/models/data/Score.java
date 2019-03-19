package uk.ac.ebi.eva.commons.models.data;

public class Score implements IScore {

    private Double score;

    private String description;

    Score() {
        //Spring empty constructor
        this(null, null);
    }

    public Score(Double score, String description) {
        this.score = score;
        this.description = description;
    }

    public Score(IScore score) {
        this(score.getScore(), score.getDescription());
    }

    @Override
    public Double getScore() {
        return score;
    }

    @Override
    public String getDescription() {
        return description;
    }

}
