use warnings;
use strict;
use IO::File;

use Getopt::Long;
my $file = "/tmp/default_mockvep_writeTofile.txt";
my $batchSize = 2;
my $result = GetOptions (
        "o=s" => \$file, # -o string
        "buffer_size=i" => \$batchSize # -b integer
        );

my $fileHandle;
if  ($file eq "STDOUT") {
    $fileHandle = IO::Handle->new();
    $fileHandle->fdopen(fileno(STDOUT),"w");
} else {
    $fileHandle = new IO::File;
    $fileHandle->open(">> $file");
}

print $fileHandle "## This line acts as the beginning of the header\n";
print $fileHandle "## More header lines\n";
print $fileHandle "# This line acts as the end of the header\n";
my @buffer = ();
my $line;
while ($line = <STDIN>) {
    chomp ($line);
    push (@buffer, "$line annotated\n");
    my $bufferSize = scalar (@buffer);
    if ($bufferSize == $batchSize) {
        foreach my $bufferLine (@buffer) {
            print $fileHandle $bufferLine;
        }
        @buffer = ();
        $fileHandle->flush();
    }
}

foreach my $bufferLine (@buffer) {
    print $fileHandle $bufferLine;
}

print $fileHandle "extra line as if some variant had two annotations\n";
$fileHandle->close();
