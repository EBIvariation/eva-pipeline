package uk.ac.ebi.eva.pipeline.io.contig;

import uk.ac.ebi.eva.pipeline.io.AssemblyReportReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.util.StringUtils.hasText;

public class ContigMapping {

    private static final String ASSEMBLED_MOLECULE = "assembled-molecule";

    private static final String NOT_AVAILABLE = "na";

    Map<String, ContigSynonyms> sequenceNameToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> assignedMoleculeToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> genBankToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> refSeqToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> ucscToSynonyms = new HashMap<>();

    public ContigMapping(String assemblyReportUrl) throws Exception {
        this(new AssemblyReportReader(assemblyReportUrl));
    }

    public ContigMapping(AssemblyReportReader assemblyReportReader) throws Exception {
        ContigSynonyms contigSynonyms;
        while ((contigSynonyms = assemblyReportReader.read()) != null) {
            fillContigConventionMaps(contigSynonyms);
        }
    }

    public ContigMapping(List<ContigSynonyms> contigSynonyms) {
        contigSynonyms.forEach(this::fillContigConventionMaps);
    }

    /**
     * Adds an entry to every map, where the key is a name, and the value is the row where it appears.
     * <p>
     * Take into account:
     * - UCSC and assignedMolecule columns may appear as "na" (not available).
     * - assignedMolecule values may not be unique across rows. Keep only those that have "assembled-molecule" in the
     * Sequence-Role column.
     * - with our custom assembly reports, sometimes the Genbank column is also not unique. Keep all the duplicated
     * entries but when asked for a genbank, return the mapping where it's an "assembled-molecule".
     */
    private void fillContigConventionMaps(ContigSynonyms contigSynonyms) {
        normalizeNames(contigSynonyms);

        if (contigSynonyms.getSequenceName() != null) {
            ContigSynonyms previousValue = sequenceNameToSynonyms.put(contigSynonyms.getSequenceName(), contigSynonyms);
            if (previousValue != null) {
                throw new IllegalArgumentException(
                        "Can't build a contig mapping because the sequence names (chromosome names) such as '"
                                + contigSynonyms.getSequenceName() + "' are not unique.");
            }
        }
        if (contigSynonyms.getAssignedMolecule() != null) {
            assignedMoleculeToSynonyms.put(contigSynonyms.getAssignedMolecule(), contigSynonyms);
        }
        if (contigSynonyms.getGenBank() != null) {
            if (ASSEMBLED_MOLECULE.equals(contigSynonyms.getSequenceRole())) {
                genBankToSynonyms.put(contigSynonyms.getGenBank(), contigSynonyms);
            } else {
                genBankToSynonyms.putIfAbsent(contigSynonyms.getGenBank(), contigSynonyms);
            }
        }
        if (contigSynonyms.getRefSeq() != null) {
            refSeqToSynonyms.put(contigSynonyms.getRefSeq(), contigSynonyms);
        }
        if (contigSynonyms.getUcsc() != null) {
            ucscToSynonyms.put(contigSynonyms.getUcsc(), contigSynonyms);
        }

    }

    private void normalizeNames(ContigSynonyms contigSynonyms) {
        if (NOT_AVAILABLE.equals(contigSynonyms.getSequenceName())) {
            contigSynonyms.setSequenceName(null);
        }
        if (NOT_AVAILABLE.equals(contigSynonyms.getAssignedMolecule())
                || !ASSEMBLED_MOLECULE.equals(contigSynonyms.getSequenceRole())) {
            contigSynonyms.setAssignedMolecule(null);
        }
        if (NOT_AVAILABLE.equals(contigSynonyms.getGenBank())) {
            contigSynonyms.setGenBank(null);
        }
        if (NOT_AVAILABLE.equals(contigSynonyms.getRefSeq())) {
            contigSynonyms.setRefSeq(null);
        }
        if (NOT_AVAILABLE.equals(contigSynonyms.getUcsc())) {
            contigSynonyms.setUcsc(null);
        }
    }

    public ContigSynonyms getContigSynonyms(String contig) {
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = refSeqToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = genBankToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = assignedMoleculeToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = sequenceNameToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = ucscToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        return null;
    }

    public String getContigSynonym(String contig, ContigSynonyms contigSynonyms, ContigNaming contigNaming) {
        if (contigSynonyms == null) {
            return null;
        }
        switch (contigNaming) {
            case SEQUENCE_NAME:
                return contigSynonyms.getSequenceName();
            case ASSIGNED_MOLECULE:
                return contigSynonyms.getAssignedMolecule();
            case INSDC:
                return contigSynonyms.getGenBank();
            case REFSEQ:
                return contigSynonyms.getRefSeq();
            case UCSC:
                return contigSynonyms.getUcsc();
            case NO_REPLACEMENT:
                return contig;
            default:
                throw new RuntimeException(
                        "Enum type " + ContigNaming.class.getSimpleName() + " doesn't accept value "
                                + contigNaming);
        }
    }

    /**
     * Replacement with a GenBank sequence accession is only possible if:
     * - Contig has synonyms
     * - GenBank and RefSeq are identical or are not identical but there is no RefSeq accession. This means that no
     * replacement will be done with a line like "chr1 ... genbank1 <> refseq1 ...", not even chr -> genbank
     * - Contig has a GenBank synonym
     */
    public boolean isGenbankReplacementPossible(String contig, ContigSynonyms contigSynonyms, StringBuilder reason) {
        if (contigSynonyms == null) {
            reason.append("Contig '" + contig + "' was not found in the assembly report!");
            return false;
        }

        if (!contigSynonyms.isIdenticalGenBankAndRefSeq() && hasText(contigSynonyms.getRefSeq())) {
            reason.append("GenBank and RefSeq not identical in the assembly report for contig '" + contig
                    + "' and RefSeq is not empty. No conversion performed, even unrelated to RefSeq (e.g. "
                    + "chromosome to GenBank");
            return false;
        }

        if (!hasText(contigSynonyms.getGenBank())) {
            reason.append("No Genbank equivalent found for contig '" + contig
                    + "' in the assembly report");
            return false;
        }
        return true;
    }
}
