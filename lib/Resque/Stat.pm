package Resque::Stat;
use Any::Moose;

# ABSTRACT: The stat subsystem. Used to keep track of integer counts.

=attr resque
=cut
has resque => (
    is       => 'ro',
    required => 1,
    handles  => [qw/ redis key /]
);

=method get

Returns the int value of a stat, given a string stat name.

=cut
sub get {
    my ($self, $stat) = @_;
    $self->redis->get( $self->key( stat => $stat ) ) || 0;
}

=method incr

For a string stat name, increments the stat by one.
 
Can optionally accept a second int parameter. The stat is then
incremented by that amount.

=cut
sub incr {
    my ( $self, $stat, $by ) = @_;
    $by ||= 1;
    $self->redis->incrby( $self->key( stat => $stat ), $by );
}

=method decr

For a string stat name, decrements the stat by one.
 
Can optionally accept a second int parameter. The stat is then
decremented by that amount.

=cut
sub decr {
    my ( $self, $stat, $by ) = @_;
    $by ||= 1;
    $self->redis->decrby( $self->key( stat => $stat ), $by );
}

=method clear

Removes a stat from Redis, effectively setting it to 0.

=cut
sub clear {
    my ( $self, $stat ) = @_;
    $self->redis->del( $self->key( stat => $stat ) );
}

__PACKAGE__->meta->make_immutable();

