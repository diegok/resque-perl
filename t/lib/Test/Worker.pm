package # hide from cpan
    Test::Worker;

use strict;

sub perform {
    my $job = shift;

    $job->worker->log( $job->class . ' is processing a job' );
    sleep $job->args->[1] if $job->args->[1];
    $job->worker->log( $job->class . ' finished job' );
    $job->args->[0];
}

1;
