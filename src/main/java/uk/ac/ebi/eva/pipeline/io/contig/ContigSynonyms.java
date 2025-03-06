package uk.ac.ebi.eva.pipeline.io.contig;

public class ContigSynonyms {

    private String sequenceName;

    private String sequenceRole;

    private String assignedMolecule;

    private String genBank;

    private String refSeq;

    private String ucsc;

    private boolean identicalGenBankAndRefSeq;

    public ContigSynonyms(String sequenceName, String sequenceRole, String assignedMolecule, String genBank,
                          String refSeq, String ucsc, boolean identicalGenBankAndRefSeq) {
        this.sequenceName = sequenceName;
        this.sequenceRole = sequenceRole;
        this.assignedMolecule = assignedMolecule;
        this.genBank = genBank;
        this.refSeq = refSeq;
        this.ucsc = ucsc;
        this.identicalGenBankAndRefSeq = identicalGenBankAndRefSeq;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public String getSequenceRole() {
        return sequenceRole;
    }

    public void setSequenceRole(String sequenceRole) {
        this.sequenceRole = sequenceRole;
    }

    public String getAssignedMolecule() {
        return assignedMolecule;
    }

    public void setAssignedMolecule(String assignedMolecule) {
        this.assignedMolecule = assignedMolecule;
    }

    public String getGenBank() {
        return genBank;
    }

    public void setGenBank(String genBank) {
        this.genBank = genBank;
    }

    public String getRefSeq() {
        return refSeq;
    }

    public void setRefSeq(String refSeq) {
        this.refSeq = refSeq;
    }

    public String getUcsc() {
        return ucsc;
    }

    public void setUcsc(String ucsc) {
        this.ucsc = ucsc;
    }

    public boolean isIdenticalGenBankAndRefSeq() {
        return identicalGenBankAndRefSeq;
    }

    public void setIdenticalGenBankAndRefSeq(boolean identicalGenBankAndRefSeq) {
        this.identicalGenBankAndRefSeq = identicalGenBankAndRefSeq;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContigSynonyms)) {
            return false;
        }

        ContigSynonyms that = (ContigSynonyms) o;

        if (identicalGenBankAndRefSeq != that.identicalGenBankAndRefSeq) {
            return false;
        }
        if (sequenceName != null ? !sequenceName.equals(that.sequenceName) : that.sequenceName != null) {
            return false;
        }
        if (sequenceRole != null ? !sequenceRole.equals(that.sequenceRole) : that.sequenceRole != null) {
            return false;
        }
        if (assignedMolecule != null ? !assignedMolecule.equals(
                that.assignedMolecule) : that.assignedMolecule != null) {
            return false;
        }
        if (genBank != null ? !genBank.equals(that.genBank) : that.genBank != null) {
            return false;
        }
        if (refSeq != null ? !refSeq.equals(that.refSeq) : that.refSeq != null) {
            return false;
        }
        return ucsc != null ? ucsc.equals(that.ucsc) : that.ucsc == null;
    }

    @Override
    public int hashCode() {
        int result = sequenceName != null ? sequenceName.hashCode() : 0;
        result = 31 * result + (sequenceRole != null ? sequenceRole.hashCode() : 0);
        result = 31 * result + (assignedMolecule != null ? assignedMolecule.hashCode() : 0);
        result = 31 * result + (genBank != null ? genBank.hashCode() : 0);
        result = 31 * result + (refSeq != null ? refSeq.hashCode() : 0);
        result = 31 * result + (ucsc != null ? ucsc.hashCode() : 0);
        result = 31 * result + (identicalGenBankAndRefSeq ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ContigSynonyms{" +
                "sequenceName='" + sequenceName + '\'' +
                ", assignedMolecule='" + assignedMolecule + '\'' +
                ", sequenceRole='" + sequenceRole + '\'' +
                ", genBank='" + genBank + '\'' +
                ", refSeq='" + refSeq + '\'' +
                ", ucsc='" + ucsc + '\'' +
                ", identicalGenBankAndRefSeq=" + identicalGenBankAndRefSeq +
                '}';
    }
}
