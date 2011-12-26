package Resque::Failure::Redis;
use Any::Moose;
with 'Resque::Failure';
with 'Resque::Encoder';

# ABSTRACT: Redis backend for worker failures

sub save {
    my $self = shift;
    my $data = $self->encoder->encode({
        failed_at => $self->failed_at,
        payload   => $self->job->as_hashref,
        exception => $self->exception,
        error     => $self->error,
        backtrace => $self->backtrace,
        worker    => $self->worker->stringify,
        queue     => $self->queue
    });
    $self->resque->redis->rpush( $self->resque->key( 'failed' ), $data );
}

__PACKAGE__->meta->make_immutable;
