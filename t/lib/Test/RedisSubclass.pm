package # hide from cpan
    Test::RedisSubclass;

use Moose;
extends 'Test::RedisMoose';

# A test subclass for the Redis module, this should still
# pass the duck_type validation

sub new {
    my $class = shift;
    return bless {}, $class;
}

1;
