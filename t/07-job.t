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
    my $args   = [+{ test_arg1 => 'hoge' }, +{ test_arg2 => 'huga' }];

    my $job = Resque::Job->new(
        +{
            resque  => $resque,
            payload => +{
                class => $worker,
                args  => $args,
            }
        }
    );

    isa_ok( $job, 'Resque::Job' );
    is $job->class, $worker;
    is_deeply $job->args, $args;
    is_deeply $job->payload, +{
        class => $worker,
        args  => $args,
    }, 'Build payload';

    $job->payload->{args} = ['please run trigger'];
    is_deeply $job->payload, +{
        class => $worker,
        args  => ['please run trigger'],
    }, 'Re build payload';

    $job->payload->{class} = 'Hoge::Worker';
    is_deeply $job->payload, +{
        class => 'Hoge::Worker',
        args  => ['please run trigger'],
    }, 'Re build payload';

}

done_testing();
