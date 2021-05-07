use Test::More;
use Resque;
use lib 't/lib';
use Test::SpawnRedisServer;

my ($c, $server) = redis();
END { $c->() if $c }

{
    ok ( my $r = Resque->new( redis => { server => $server }, namespace => 'test_resque' ), "Building object for test server $server" );
    ok ( $r->redis, 'Has redis object' );
    ok ( $r->redis->ping, 'Redis object is alive' );
    is ( ref $r->redis eq 'Redis::Fast' ? $r->redis->__get_reconnect : $r->redis->{reconnect}, 60, 'Default parameters are loaded' );
}

{
    ok ( my $r = Resque->new( redis => { server => $server, reconnect => 120 }, namespace => 'test_resque' ), "Building object for test server $server" );
    ok ( $r->redis, 'Has redis object' );
    ok ( $r->redis->ping, 'Redis object is alive' );
    is ( ref $r->redis eq 'Redis::Fast' ? $r->redis->__get_reconnect : $r->redis->{reconnect}, 120, 'Default values have been overwritten' );
}

done_testing();
