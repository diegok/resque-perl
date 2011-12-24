package Resque::Job;
use Any::Moose;

# ABSTRACT: Resque job container

use JSON;

=attr resque
=cut
has resque  => ( 
    is      => 'rw', 
    default => sub { confess "This Resque::Job isn't associated to any Resque system yet!" } 
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

=attr encoder
  JSON encoder by default.
=cut
has encoder => ( is => 'ro', default => sub { JSON->new->utf8 } );

=attr payload
  Restore the job from encoded state.
  This is read-only.
=cut
has payload => ( 
    is  => 'ro', 
    isa => 'Str', 
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
    $self->encoder->encode({
        class => $self->class,
        args  => $self->args
    });
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
    confess "unable to perform yet!";
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

__PACKAGE__->meta->make_immutable();

