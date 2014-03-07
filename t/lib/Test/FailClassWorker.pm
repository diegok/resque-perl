package # hide from cpan
    Test::FailClassWorker;

use strict;
use DateTime;

sub perform {
    my $job = shift;
    die DateTime->now;
}

1;
