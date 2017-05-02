# mockvep_writeToFile_delayed.pl
#
# This file is a mock for VEP, just as mockvep_writeToFile.pl, with the difference that it waits for 3 seconds before
# writing the last annotation and closing stdout. This is used for testing that we handle properly the cases when:
# 1: VEP is slow and needs a bit more time to finish annotating.
# 2: Some of the threads of VEP died but the parent didn't notice, so VEP will block forever and we have to kill it.

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

sleep 3;
print $fileHandle "extra line as if some variant had two annotations\n";

$fileHandle->close();
