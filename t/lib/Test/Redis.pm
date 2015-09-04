package # hide from cpan
    Test::Redis;

# A dummy package for test 06-other-redis-classes.t, which
# tests that Resque can accept classes other than Redis as
# its redis object. This gives us support for Redis::Fast, or
# for any other class that implements a Redis client.

sub new {
    my $class = shift;
    return bless {}, $class;
}

1;
