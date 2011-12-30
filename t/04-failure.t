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
    $w->work_tick($w->reserve);
    is( $w->failed, 1, 'One failure reported on this worker' );
    $w->work_tick($w->reserve);
    is( $w->failed, 2, 'Two failure reported on this worker' );

    is( $r->failures->count, 2, 'There was two failures in all system' );
    is( my @fails = $r->failures->all(0,-1), 2, 'Get all() failures' );
}

sub push_job {
    my $r = shift;
    my $class = shift || 'Test::FailWorker';
    ok( $r->push( test => { class => $class, args => [ 'ouch!' ] } ),    'Push fail job to test queue' ); 
    ok( $r->push( test => { class => $class, args => [ 'bazinga!' ] } ), 'Push fail job to test queue' ); 
}

done_testing();
