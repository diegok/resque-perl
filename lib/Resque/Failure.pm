package Resque::Failure;
use Any::Moose 'role';
use overload '""' => \&stringify;

requires 'requeue';
requires 'remove';
requires 'save';

has 'worker' => ( 
    isa      => 'Resque::Worker', 
    required => 1
);

has 'job' => ( 
    isa      => 'Resque::Worker', 
    handles  => { 
        resque  => 'resque', 
        requeue => 'enqueue',
        payload => 'encode',
        encoder => 'encoder',
        queue   => 'queue',
    },
    required => 1
);

has created => ( 
    is      => 'rw',
    default => { DateTime->now } 
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
has error      => ( is => 'rw', isa => 'Str', required => 1 );
has stacktrace => ( is => 'rw', isa => 'Str' ); 

around error => sub {
    my $orig = shift;
    my $self = shift;

    return $self->$orig() unless @_;

    my ( $value, @stack ) = split "\n", shift;
    $self->stacktrace( join "\n", @stack );
    return $self->$orig($value);
};


sub stringify { $_[0]->error }

__PACKAGE__->meta->make_immutable;
