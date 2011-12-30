package # hide from cpan
    Test::FailWorker;

use strict;
use 5.10.1;

sub perform {
    my $job = shift;
    die "Bye bye cruel world!";
}

1;
