package # hide from cpan
    Test::FailWorker;

use strict;
use Carp;

sub perform {
    my $job = shift;
    Carp::confess "Bye bye cruel world!";
}

1;
