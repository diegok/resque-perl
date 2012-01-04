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
    isa_ok( my $worker = $r->worker, 'Resque::Worker' );
    ok( $worker->add_queue( 'test' ), 'Listen to test queue' );
    is( $worker->queues->[0], 'test', 'Worker know about the queue' );
    is( @{$worker->queues}, 1, 'Worker listen to one queue' );
    ok( $worker->add_queue( 'test' ), 'Add the same queue' );
    is( @{$worker->queues}, 1, 'Worker still listen to one queue' );
    ok( $worker->add_queue( 'test2', 'test3' ), 'Add two more queues' );
    is( @{$worker->queues}, 3, 'Worker has 3 queues' );
    for my $name (qw/ test test2 test3 test /) {
        is( $worker->next_queue, $name, 'Order of queues is ok and roundtrip' )
    }
    is( $worker->del_queue( 'test' ), 1, 'Delete one queue' );
    is( @{$worker->queues}, 2, 'Worker has 2 queues' );
    is( $worker->del_queue( 'test' ), 0, 'Delete nothing' );

    ok( $worker == $worker, 'Worker respond to ==' );
    ok( $worker eq $worker, 'Worker respond to eq' );


    is( @{ $worker->all }, 0, 'No workers registered' );
    $worker->register_worker;
    is( @{ $worker->all }, 1, 'One worker registered' );

    ok( ! $worker->reserve, 'Nothing to reserve()' );
    push_job($r);
    ok( my $job = $worker->reserve, 'reserve() a job' );
    is( $job->args->[0], 'bazinga!', 'Is first job in first queue' );
    is( $job->queue, 'test2', 'Job object known about queue' );
    ok( ! $job->has_worker, 'No worker set on job' );
    
    $worker->working_on($job);
    ok( $job->has_worker, 'Worker set on job after working_on' );
    is( $worker->processing->{queue}, 'test2', 'processing() know what worker is doing');
    ok( $worker->is_working, 'Worker is working' );
    ok( ! $worker->is_idle, 'Worker is not idle' );
    is( $worker->perform($job), 'bazinga!', 'Worker can make a job to perform()');
    $worker->done_working;

    ok( !$worker->is_working, 'Worker is not working' );
    ok( $worker->is_idle, 'Worker is idle' );

    ok( $job = $worker->reserve, 'reserve() a job' );
    is( $job->args->[0], 'ouch!', 'Is first job in seccond queue' );

    ok( $worker->cant_fork(1), 'Prevent worker from fork()');
    $worker->work_tick($job);
    ok( !$worker->is_working, 'Worker is not working' );
    ok( $worker->is_idle, 'Worker is idle' );
    ok( ! $worker->reserve, 'No more jobs on any queue' );
}

sub push_job {
    my $r = shift;
    my $class = shift || 'Test::Worker';
    ok( $r->push( test3 => { class => $class, args => [ 'ouch!' ] } ),    'Push new job to test3 queue' ); 
    ok( $r->push( test2 => { class => $class, args => [ 'bazinga!' ] } ), 'Push new job to test2 queue' ); 
}

done_testing();
