package Resque::Job;
# ABSTRACT: Resque job container

use Moose;
use Moose::Util::TypeConstraints;
with 'Resque::Encoder';

use overload '""' => \&stringify;
use Class::Load qw(load_class);

=attr resque

Provides 'redis' method, which provides access to our redis subsystem.

=cut
has resque  => (
    is      => 'rw',
    handles => [qw/ redis /],
    default => sub { confess "This Resque::Job isn't associated to any Resque system yet!" }
);

=attr worker

Worker running this job.
A new worker will be popped up from resque by default.

=cut
has worker  => (
    is      => 'rw',
    lazy    => 1,
    default   => sub { $_[0]->resque->worker },
    predicate => 'has_worker'
);

=attr class

Class to be performed by this job.

=cut
has class => ( is => 'rw', lazy => 1, default => sub { confess "This job needs a class to do some work." } );

=attr queue

Name of the queue this job is or should be.

=cut
has queue => (
    is        => 'rw', lazy => 1,
    default   => \&queue_from_class,
    predicate => 'queued'
);

=attr args

Array of arguments for this job.

=cut
has args => ( is => 'rw', isa => 'ArrayRef', default => sub {[]} );

=attr payload

HashRef representation of the job.
When passed to constructor, this will restore the job from encoded state.
When passed as a string this will be coerced using JSON decoder.
This is read-only.

=cut
coerce 'HashRef'
    => from 'Str'
    => via { JSON->new->utf8->decode($_) };
has payload => (
    is      => 'ro',
    isa     => 'HashRef',
    coerce  => 1,
    lazy    => 1,
    builder => 'payload_builder',
    trigger => \&_payload_trigger
);

=method encode

String representation(JSON) to be used on the backend.

    $job->encode();

=cut
sub encode {
    my $self = shift;
    $self->encoder->encode( $self->payload );
}

=method stringify

Returns a string version of the job, like

'(Job{queue_name) | ClassName | args_encoded)'

    my $stringified = $job->stringify();

=cut
sub stringify {
    my $self = shift;
    sprintf( "(Job{%s} | %s | %s)",
        $self->queue,
        $self->class,
        $self->encoder->encode( $self->args )
    );
}

=method queue_from_class

Normalize class name to be used as queue name.

    my $queue_name = $job->queue_from_class();

    NOTE: future versions will try to get the
          queue name from the real class attr
          or $class::queue global variable.

=cut
sub queue_from_class {
    my $self = shift;
    my $class = $self->class;
    $class =~ s/://g;
    $class;
}

=method perform

Load job class and call perform() on it.
This job object will be passed as the only argument.

    $job->perform();

=cut
sub perform {
    my $self = shift;
    load_class($self->class);
    $self->class->can('perform')
        || confess $self->class . " doesn't know how to perform";

    no strict 'refs';
    &{$self->class . '::perform'}($self);
}

=method enqueue

Add this job to resque.
See Rescue::push().

    $job->enqueue();

=cut
sub enqueue {
    my $self = shift;
    $self->resque->push( $self->queue, $self );
}

=method dequeue

Remove this job from resque using the most restrictive
form of Resque::mass_dequeue.
This method will remove all jobs matching this
object queue, class and args.

See Resque::mass_dequeue() for massive destruction.

    $job->enqueue();

=cut
sub dequeue {
    my $self = shift;
    $self->resque->mass_dequeue({
        queue => $self->queue,
        class => $self->class,
        args  => $self->args
    });
}

=method fail

Store a failure (or exception and failure) on this job.

    $job->fail( "error message'); # or
    $job->fail( ['exception', 'error message'] );

=cut
sub fail {
    my ( $self, $error ) = @_;

    my $exception = 'Resque::Failure::Job';
    if ( ref $error && ref $error eq 'ARRAY' ) {
        ( $exception, $error ) = @$error;
    }

    $self->resque->throw(
        job       => $self,
        worker    => $self->worker,
        queue     => $self->queue,
        payload   => $self->payload,
        exception => $exception,
        error     => $error
    );
}

=method payload_builder

Default payload builder method. This method is public only to be wrapped in the
context of plugins that adds attributes to this class.

=cut
sub payload_builder {+{
    class => $_[0]->class,
    args  => $_[0]->args
}}

=method payload_reader

Default payload trigger method. This method is public only to be wrapped in the
context of plugins that adds attributes to this class.

This mehtod is only called at construction time to populate job class and args
attributes from payload.

=cut
sub payload_reader {
    my ( $self, $hr ) = @_;
    $self->class( $hr->{class} );
    $self->args( $hr->{args} ) if $hr->{args};
}

sub _payload_trigger { shift->payload_reader(@_) }

__PACKAGE__->meta->make_immutable();
