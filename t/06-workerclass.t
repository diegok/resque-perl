use Test::More;
use Resque;
use lib 't/lib';
use Test::SpawnRedisServer;

my ($c, $server) = redis();
END { $c->() if $c }

ok ( my $r = Resque->new( redis => $server, namespace => 'test_resque' ), "Building object for test server $server" );
ok ( $r->redis, 'Has redis object' );
ok ( $r->redis->ping, 'Redis object is alive' );

$r->flush_namespace;
{
    ok( $r->push( test => { class => 'Test::WorkerClass', args => [{name => 'Ambar'}] } ), 'Push a WorkerClass job to a queue' );
    isa_ok( my $worker = $r->worker, 'Resque::Worker', 'Get a worker instance' );
    ok( $worker->add_queue('test'), 'Make worker listen to our test queue' );
    ok( my $job  = $worker->reserve, 'reserve() that job' );
    isa_ok( my $task = $worker->perform($job), 'Test::WorkerClass', 'Job can perform and returns itself' );
    is( $task->name, 'Ambar', 'Background job instance attrs initialized' );
    isa_ok( $task->job, 'Resque::Job', '... and can access to the job' );
    isa_ok( $task->resque, 'Resque', '... and can access to resque' );
    isa_ok( $task->redis, 'Redis', '... and can access to redis' );
}

done_testing();
