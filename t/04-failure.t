use Test::More;
use Resque;
use lib 't/lib';
use Test::SpawnRedisServer;

my ($c, $server) = redis();
END { $c->() if $c }

ok ( my $r = Resque->new( redis => $server, namespace => 'test_resque' ), "Building object for test server $server" );
ok ( $r->redis->ping, 'Redis object is alive' );

$r->flush_namespace;
{
    ok( my $w = $r->worker, 'Setup worker' );
    ok( $w->add_queue( 'test' ), 'Add queue' );
    push_job($r);

    $w->cant_fork(1);
    is( $r->failures->count, 0, 'There is no failures in all system' );

    is( $w->failed, 0, 'No failure reported on this worker' );
    ok( !$w->work_tick($w->reserve), 'Work one time' );
    is( $w->failed, 1, 'One failure reported on this worker' );
    ok( !$w->work_tick($w->reserve), 'Work one time' );
    is( $w->failed, 2, 'Two failure reported on this worker' );

    is( $r->failures->count, 2, 'There is two failures in all system' );

    ok( $r->failures->remove(0), 'Remove first failure' );
    is( $r->failures->count, 1, 'There is one failure now' );

    ok( $r->failures->requeue(0), 'Requeue first failure' );
    ok( !$w->work_tick($w->reserve), 'Call the worker one more time' );
    is( $w->failed, 3, 'Three failure reported on this worker' );
    is( my @fails = $r->failures->all(0,-1), 2, 'Get all() two failures' );
    ok( $fails[0]->{retried_at}, 'First one has been retried' );
    ok( ! $fails[1]->{retried_at}, 'Seccond one has not been retried' );
    ok( $fails[0]->{backtrace}, 'parse error and set backtrace' ) or diag explain $fails[0];
    ok( ref $fails[0]->{backtrace} eq 'ARRAY', 'backtrace is ArrayRef. for resque-web' );
    ok( $fails[0]->{error} !~ /\n/, '$fail->{error} have no "\n"') or diag $fails[0]->{error};
}

sub push_job {
    my $r = shift;
    my $class = shift || 'Test::FailWorker';
    ok( $r->push( test => { class => $class, args => [ 'ouch!' ] } ),    'Push fail job to test queue' );
    ok( $r->push( test => { class => $class, args => [ 'bazinga!' ] } ), 'Push fail job to test queue' );
}

done_testing();
