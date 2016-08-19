package # hide from cpan
    Test::WorkerClass;

use Resque::WorkerClass;

has name => is => 'ro';

sub run { shift }

1;
