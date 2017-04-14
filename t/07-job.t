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
    my $worker = 'Test::Worker';
    my $args   = [{ test_arg1 => 'hoge' }, { test_arg2 => 'huga' }];

    ok( my $job = Resque::Job->new( resque => $resque, class => $worker, args => $args ), 'Build Resque::Job by setting class and args' );
    is_deeply $job->payload, { class => $worker, args => $args }, 'Payload is ok';

    ok( $job = Resque::Job->new( resque  => $resque, payload => { class => $worker, args => $args } ), 'Build Resque::Job by setting payload' );
    isa_ok( $job, 'Resque::Job' );
    is $job->class, $worker, 'Job class is ok';
    is_deeply $job->args, $args, 'Job args are ok';
}

done_testing();
