package Resque::Failures;
use Any::Moose;
with 'Resque::Encoder';

use UNIVERSAL::require;
use Carp;

=attr resque
Accessor to the Resque object.
=cut
has resque => ( 
    is       => 'ro', 
    required => 1, 
    handles  => [qw/ redis key /]
);

=attr failure_class
Name of a class consuming the role 'Resque::Failure'.
By default: Resque::Failure::Redis
=cut
has failure_class => (
    is => 'rw', 
    lazy => 1, 
    default => sub { 
        'Resque::Failure::Redis'->require || confess $@;
        'Resque::Failure::Redis'; 
    },
    trigger => sub {
        my ( $self, $class ) = @_;
        $class->require or confess $@;
    }
);

=method throw
create() a failure on the failure_class() and save() it.
=cut
sub throw {
    my $self = shift;
    my $e = $self->create(@_);
    $e->save;
}

=method create
Create a new failure on the failure_class() backend.
=cut
sub create {
    my $self = shift;
    $self->failure_class->new( @_, resque => $self->resque );
}

=method count
How many failures was in all the resque system.
=cut
sub count {
    my $self = shift;
    $self->redis->llen($self->key('failed'));
}

=method all
Return a range of failures in the same way Resque::peek() does for
jobs.
=cut
sub all {
    my ( $self, $start, $count ) = @_;
    my $all = $self->resque->list_range(
        $self->key('failed'), $start, $count
    );
    $_ = $self->encoder->decode( $_ ) for @$all;
    return wantarray ? @$all : $all;
}

=method clear
Remove all failures.
=cut
sub clear {
    my $self = shift;
    $self->redis->del($self->key('failed'));
}

=method requeue
=cut
sub requeue {
    my ( $self, $index ) = @_;
    my $item = $self->all($index);
    $item->{retried_at} = DateTime->now->strftime("%Y/%m/%d %H:%M:%S");
    $self->redis->lset(
        $self->key('failed'), $index, 
        $self->encoder->encode($item)
    );
    $self->resque->push(
        $item->{queue} => { 
            class => $item->{payload}{class}, 
            args  => $item->{payload}{args}, 
    });
}

=method remove
=cut
sub remove {
    my ( $self, $index ) = @_;
    my $id = rand(0xffffff);
    my $key = $self->key('failed');
    $self->redis->lset( $key, $index, $id);
    $self->redis->lrem( $key, 1, $id );
}

__PACKAGE__->meta->make_immutable();
