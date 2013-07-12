package # hide from cpan
    Test::LongRunningWorker;

use strict;
use Carp;

sub perform {
    my $job = shift;
    
    sleep 10;
}

1;
