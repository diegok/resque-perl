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

# push
push_jobs($r);
is( $r->size('test'), 2, 'Test queue has two jobs'); 

{ # peek jobs
    ok( my @jobs = $r->peek('test', 0, -1), 'peek() all test items');
    is( @jobs, 2, 'test queue has two items' );
    isa_ok( $jobs[0], 'Resque::Job' );
    is( $jobs[0]->args->[0], 1, 'Job arguments looks good');
    is( @jobs = $r->peek('test', 0, 1), 1, 'peek() first job');
    is( $jobs[0]->args->[0], 1, 'Job arguments looks good');
    is( @jobs = $r->peek('test', 1, 1), 1, 'peek() seccond job');
    is( $jobs[0]->args->[0], 2, 'Job arguments looks good');
}

{ # queues
    ok( my @queues = $r->queues, 'Get queues');
    is( @queues, 1, 'There is one queue');
    is( $queues[0], 'test', 'Queue name is test');
}

{  # pop 
    ok( my $job = $r->pop('test'), 'Pop a job from the test queue' );
    is( $r->size('test'), 1, 'Test queue has one job left'); 
    isa_ok( $job, 'Resque::Job' );
    is( $job->args->[0], 1, 'Job argument looks good' );
    is( $job->queue, 'test', "Job known about it's queue" );

    ok( $job = $r->pop('test'), 'Pop other job from the test queue' );
    is( $r->size('test'), 0, 'Test queue is empty'); 
    isa_ok( $job, 'Resque::Job' );
    is( $job->args->[0], 2, 'Job argument looks good' );

    ok( ! $r->pop('test'), 'Pop a job from test queue returns false' );
}

# remove queue
is( @{$r->queues}, 1, 'There one queue');
$r->remove_queue('test');
is( @{$r->queues}, 0, 'There is no queues after remove_queue()');

# remove queue with jobs
push_jobs($r);
is( @{$r->queues}, 1, 'There one queue with jobs');
$r->remove_queue('test');
is( @{$r->queues}, 0, 'There is no queues after remove_queue()');
ok( ! $r->pop('test'), "Removed queue's don't pop()" );

# mass dequeue on empty set
is( $r->mass_dequeue({
    queue => 'test',
    class => 'OtherTask'
}), 0, 'dequeue no jobs on non existant queue' );  


# dequeue
push_jobs($r);
is( $r->size('test'), 2, 'Test queue has two jobs again'); 
isa_ok( $r->peek('test', 0, 1)->[0], 'Resque::Job' );
is( $r->peek('test', 0, 1)->[0]->dequeue, 1, 'dequeue single job' );  
is( $r->size('test'), 1, 'Test queue has one job'); 

# Massive destruction :-p
push_jobs($r, 'OtherTask');
is( $r->size('test'), 3, 'Test queue has 3 jobs'); 
is( $r->mass_dequeue({
    queue => 'test',
    class => 'OtherTask'
}), 2, 'dequeue two jobs' );  
is( $r->size('test'), 1, 'Test queue has one job'); 

# mass dequeue on non empty without match
is( $r->mass_dequeue({
    queue => 'test',
    class => 'OtherTask'
}), 0, 'dequeue no jobs on non existant queue' );  

sub push_jobs {
    my $r = shift;
    my $class = shift || 'TestWorker';
    ok( $r->push( test => { class => $class, args => [ 1, { test => 'ok' } ] } ), 'Push new job to test queue' ); 
    ok( $r->push( test => { class => $class, args => [ 2, { test => 'ok' } ] } ), 'Push another job to test queue' ); 
}

done_testing();
