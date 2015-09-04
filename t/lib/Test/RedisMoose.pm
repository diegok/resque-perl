package # hide from cpan
    Test::RedisMoose;

use Moose;

# A dummy package for test 06-other-redis-classes.t, which
# tests that Resque can accept classes other than Redis as
# its redis object. This gives us support for Redis::Fast, or
# for any other class that implements a Redis client.
# This is like TestRedis, but is a Moose class.

__PACKAGE__->meta->make_immutable;
1;
