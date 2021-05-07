use Test::More;
use Resque;
use lib 't/lib';
use Test::SpawnRedisServer;

my ($c, $server) = redis();
END { $c->() if $c }

{
    ok ( my $r = Resque->new( redis => $server, namespace => 'test_resque' ), "Build object passing $server as a string" );
    ok ( $r->redis, 'Has redis object' );
    ok ( $r->redis->ping, 'Redis object is alive' );
}

{
    ok ( my $r = Resque->new( redis => { server => $server, reconnect => 120 }, namespace => 'test_resque' ), "Building object passing a hashref with redis args" );
    ok ( $r->redis, 'Has redis object' );
    ok ( $r->redis->ping, 'Redis object is alive' );
}

done_testing();
