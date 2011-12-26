package Resque::Job;
use Any::Moose;
with 'Resque::Encoder';

# ABSTRACT: Resque job container

use overload '""' => \&stringify;

=attr resque
=cut
has resque  => ( 
    is      => 'rw', 
    default => sub { confess "This Resque::Job isn't associated to any Resque system yet!" } 
);

=attr worker
=cut
has worker  => ( 
    is      => 'rw', 
    lazy    => 1,
    default   => sub { $_[0]->resque->worker },
    predicate => 'has_worker'
);

=attr class
=cut
has class   => ( is => 'rw', lazy => 1, default => sub { confess "This job needs a class to do some work." } );

=attr queue
=cut
has queue   => ( 
    is        => 'rw', lazy => 1, 
    default   => \&queue_from_class, 
    predicate => 'queued'
);

=attr args
=cut
has args    => ( is => 'rw', isa => 'ArrayRef', default => sub {[]} );

=attr payload
  Job encoded() representation.
  When passed to constructor, this will restore the job from encoded state.
  This is read-only.
=cut
has payload => ( 
    is   => 'ro', 
    isa  => 'Str', 
    lazy => 1,
    default => sub { $_[0]->encode },
    trigger => sub {
        my ( $self, $value ) = @_;
        my $hr = $self->encoder->decode( $value );
        $self->class( $hr->{class} );
        $self->args( $hr->{args} ) if $hr->{args};
    }
);

=method encode
  String representation to be used on 'payload'
  constructor argument of this object.
=cut
sub encode {
    my $self = shift;
    $self->encoder->encode( $self->as_hashref );
}

sub as_hashref {
    my $self = shift;
    return {
        class => $self->class,
        args  => $self->args
    };
}

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
=cut
sub perform {
    my $self = shift;
    $self->class->require || confess $@;
    $self->class->can('perform') 
        || confess $self->class . " doesn't know how to perform";

    no strict 'refs';
    &{$self->class . '::perform'}($self); 
}

=method enqueue
  Add this job to resque.
  See Rescue::push().
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
=cut
sub dequeue {
    my $self = shift;
    $self->resque->mass_dequeue({
        queue => $self->queue,
        class => $self->class,
        args  => $self->args
    });
}

sub fail {
    my ( $self, $why ) = @_;
    #run_failure_hooks(exception)
    $self->throw($why);
}

sub throw {
    my ( $self, $error ) = @_;
    $self->resque->throw(
        job       => $self,
        worker    => $self->worker,
        queue     => $self->queue,
        payload   => $self->encode,
        exception => 'Resque::Failure::Job',
        error     => $error
    );
}

__PACKAGE__->meta->make_immutable();

