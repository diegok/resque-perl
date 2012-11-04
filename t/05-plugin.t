use Test::More;
use Resque;
use lib 't/lib';
use Test::SpawnRedisServer;

my ($c, $server) = redis();
END { $c->() if $c }

{
    my $r = new_with_plugins('Empty');
    isa_ok( $r->plugins->[0], 'Resque::Plugin::Empty' );
}

{
    my $r = new_with_plugins('Duck');
    isa_ok( $r->plugins->[0], 'Resque::Plugin::Duck' );
    ok( $r->talk, 'Resque can talk like a duck now!' );

    ok( $r->worker->talk, 'Worker can also talk' );
    ok( $r->worker->walk, '... and walk :)' );

    ok( $r->push( test => { class => 'Test::Worker', args => [':)'] } ), 'Push new job to test queue' ); 

    ok( $r->worker->add_queue('test'), 'Add test queue to the worker' );
    ok( $job = $r->worker->reserve, 'reserve() a job' );
    is( $job->args->[0], ':)', 'Got the expected job' );

    ok( $job->talk, 'Job can also talk' );
    ok( $job->walk, '... and walk :)' );
}

done_testing();

sub new_with_plugins {
    my $plugins = join ', ', @_;

    ok ( my $r = Resque->new( 
            redis     => $server, 
            namespace => 'test_resque',
            plugins   => [@_]
    ), "Building object for test server $server with plugins: $plugins" );

    ok ( $r->redis, 'Has redis object' );
    ok ( $r->redis->ping, 'Redis object is alive' );

    $r->flush_namespace;

    $r;
}
