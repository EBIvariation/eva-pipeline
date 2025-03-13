package uk.ac.ebi.eva.pipeline.io.contig;

/**
 * This enum can be used to specify one of the synonyms from NCBI's assembly reports.
 */
//TODO: This file is copied from eva-accession, we should refactor this to put it in variation-commons and refer from there
public enum ContigNaming {
    SEQUENCE_NAME,  // this is the same as chromosome names
    ASSIGNED_MOLECULE,
    INSDC,  // this is the same as GenBank
    REFSEQ,
    UCSC,
    NO_REPLACEMENT  // do not use any particular naming, just keep whatever contig is provided
}
