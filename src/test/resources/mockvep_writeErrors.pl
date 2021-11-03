# Mock VEP that writes error messages to stderr before writing annotations to stdout and finishing successfully.

use warnings;
use strict;

my $line;
while ($line = <STDIN>) {
    ;
}

my $header = "## ENSEMBL VARIANT EFFECT PREDICTOR v78
#Uploaded_variation	Location	Allele	Gene	Feature	Feature_type	Consequence	cDNA_position	CDS_position	Protein_position	Amino_acids	Codons	Existing_variation	Extra
";

my $annot = "20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
";

my $errors = "Use of uninitialized value in reverse at Sequence.pm line 90.
" x 4000;

print STDOUT $header;

print STDERR $errors;

print STDOUT $annot;
