package Resque::Failure;
use Any::Moose 'Role';
with 'Resque::Encoder';

# ABSTRACT: Role to be consumed by any failure class.

use overload '""' => \&stringify;
use DateTime;

requires 'save';

has 'worker' => ( 
    is       => 'ro',
    isa      => 'Resque::Worker', 
    required => 1
);

has 'job' => ( 
    is      => 'ro', 
    handles  => { 
        resque  => 'resque', 
        requeue => 'enqueue',
        payload => 'payload',
        queue   => 'queue',
    },
    required => 1
);

has created => ( 
    is      => 'rw',
    default => sub { DateTime->now } 
);

has failed_at => ( 
    is      => 'ro',
    lazy    => 1,
    default => sub { 
        $_[0]->created->strftime("%Y/%m/%d %H:%M:%S %Z"); 
    },
    predicate => 'has_failed_at'
);

has exception => ( 
    is      => 'rw',
    lazy    => 1,
    default => sub { 'Resque::Failure' }
);

has error     => ( is => 'rw', isa => 'Str', required => 1 );
has backtrace => ( is => 'rw', isa => 'Str' ); 

around error => sub {
    my $orig = shift;
    my $self = shift;

    return $self->$orig() unless @_;

    my ( $value, @stack ) = split "\n", shift;
    $self->backtrace( join "\n", @stack );
    return $self->$orig($value);
};

sub stringify { $_[0]->error }

1;
