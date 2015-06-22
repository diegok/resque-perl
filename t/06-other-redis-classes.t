use Test::More;
use Test::Exception;
use Resque;
use lib 't/lib';
use Redis;
use Test::SpawnRedisServer;
use Test::Redis;
use Test::RedisMoose;
use Test::RedisSubclass;

my ($c, $server) = redis();
END { $c->() if $c }

{
	my $resque;
	ok($resque = Resque->new(redis => $server), "Can create a resque object with a Redis object");
    isa_ok($resque, 'Resque');
    isa_ok($resque->redis, 'Redis');
}

{
	my $redis = Test::Redis->new;
	my $resque;
	ok($resque = Resque->new(redis => $redis), "Can create a resque object with any redis object");
    isa_ok($resque, 'Resque');
    isa_ok($resque->redis, 'Test::Redis');
}

{
	my $redis = Test::RedisMoose->new;
	my $resque;
	ok($resque = Resque->new(redis => $redis), "Can create a resque object with any redis Moose object");
    isa_ok($resque, 'Resque');
    isa_ok($resque->redis, 'Test::RedisMoose');
}

{
	my $redis = Test::RedisSubclass->new;
	my $resque;
	ok($resque = Resque->new(redis => $redis), "Can create a resque object with an object subclassed from Redis");
    isa_ok($resque, 'Resque');
    isa_ok($resque->redis, 'Test::RedisSubclass');
}

done_testing();
