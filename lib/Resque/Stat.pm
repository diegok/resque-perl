package Resque::Stat;
use Any::Moose;

has resque => (
    is       => 'ro',
    required => 1,
    handles  => [qw/ redis key /]
);

# The stat subsystem. Used to keep track of integer counts.
#
#   Get a stat:  Stat[name]
#   Incr a stat: Stat.incr(name)
#   Decr a stat: Stat.decr(name)
#   Kill a stat: Stat.clear(name)

# Returns the int value of a stat, given a string stat name.
sub get {
    my ($self, $stat) = @_;
    $self->redis->get( $self->key( stat => $stat ) ) || 0;
}

# For a string stat name, increments the stat by one.
#
# Can optionally accept a second int parameter. The stat is then
# incremented by that amount.
sub incr {
    my ( $self, $stat, $by ) = @_;
    $by ||= 1;
    $self->redis->incrby( $self->key( stat => $stat ), $by );
}

# For a string stat name, decrements the stat by one.
#
# Can optionally accept a second int parameter. The stat is then
# decremented by that amount.
sub decr {
    my ( $self, $stat, $by ) = @_;
    $by ||= 1;
    $self->redis->decrby( $self->key( stat => $stat ), $by );
}

# Removes a stat from Redis, effectively setting it to 0.
sub clear {
    my ( $self, $stat ) = @_;
    $self->redis->del( $self->key( stat => $stat ) );
}

__PACKAGE__->meta->make_immutable();
