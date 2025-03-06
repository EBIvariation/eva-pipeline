package uk.ac.ebi.eva.pipeline.io.contig;

/**
 * This enum can be used to specify one of the synonyms from NCBI's assembly reports.
 */
public enum ContigNaming {
    SEQUENCE_NAME,  // this is the same as chromosome names
    ASSIGNED_MOLECULE,
    INSDC,  // this is the same as GenBank
    REFSEQ,
    UCSC,
    NO_REPLACEMENT  // do not use any particular naming, just keep whatever contig is provided
}
