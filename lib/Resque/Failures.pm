package Resque::Failures;
use Any::Moose;
with 'Resque::Encoder';

use UNIVERSAL::require;
use Carp;

has resque => ( 
    is       => 'ro', 
    required => 1, 
    handles  => [qw/ redis key /]
);

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

sub throw {
    my $self = shift;
    my $e = $self->failure_class->new( @_, resque => $self->resque );
    carp $e;
    $e->save;
}

=method count
=cut
sub count {
    $_[0]->redis->llen('failed')
}

=method all
=cut
sub all {
    my ( $self, $start, $count ) = @_;
    my $all = $self->resque->list_range('failed', $start, $count);
    $_ = $self->encoder->decode( $_ ) for @$all;
    return wantarray ? @$all : $all;
}

=method clear
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
