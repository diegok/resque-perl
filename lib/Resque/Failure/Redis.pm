package Resque::Failure::Redis;
use Any::Moose;
with 'Resque::Failure';
with 'Resque::Encoder';

# ABSTRACT: Redis backend for worker failures

=method save

Method required by L<Resque::Failure> role.

=cut
sub save {
    my $self = shift;
    my $data = $self->encoder->encode({
        failed_at => $self->failed_at,
        payload   => $self->job->payload,
        exception => $self->exception,
        error     => $self->error,
        backtrace => $self->backtrace,
        worker    => $self->worker->id,
        queue     => $self->queue
    });
    $self->resque->redis->rpush( $self->resque->key( 'failed' ), $data );
    $self->resque->redis->incr( $self->resque->key("stat:failed:" . $self->worker->id));
}

__PACKAGE__->meta->make_immutable;
