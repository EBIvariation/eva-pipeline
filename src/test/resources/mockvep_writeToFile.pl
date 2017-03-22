use warnings;
use strict;

use Getopt::Long;
my $file = "/tmp/default_mockvep_writeTofile.txt";
my $result = GetOptions ("o=s" => \$file); # -o string

open (MYFILE, ">> $file");
foreach my $line ( <STDIN> ) {
    chomp ($line);
    print MYFILE "$line annotated\n";
}
print MYFILE "extra line as if some variant had two annotations\n";
close (MYFILE);
