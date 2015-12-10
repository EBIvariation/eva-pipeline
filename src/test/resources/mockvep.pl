use warnings;
use strict;

my $line;
while (defined($line = <STDIN>)) {
  my $cline = chomp($line);
  print "$line annotated\n";
}
