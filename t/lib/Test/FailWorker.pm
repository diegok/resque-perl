package # hide from cpan
    Test::FailWorker;

use strict;

sub perform {
    my $job = shift;
    die "Bye bye cruel world!";
}

1;
