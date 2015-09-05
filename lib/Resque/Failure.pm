package Resque::Failure;
# ABSTRACT: Role to be consumed by any failure class.

use Moose::Role;
with 'Resque::Encoder';

use overload '""' => \&stringify;
use DateTime;
use Moose::Util::TypeConstraints;

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

coerce 'Str'
    => from 'Object'
    => via {"$_"};

has error     => ( is => 'rw', isa => 'Str', required => 1, coerce => 1 );
# ruby 'resque-web' expect backtrace is array.
has backtrace => ( is => 'rw', isa => 'ArrayRef[Str]' );

around error => sub {
    my $orig = shift;
    my $self = shift;

    return $self->$orig() unless @_;

    my ( $value, @stack ) = split "\n", shift;
    $self->backtrace( \@stack );
    return $self->$orig($value);
};

=method BUILD
=cut
sub BUILD {
    my $self = shift;
    if ( (my $error = $self->error) =~ /\n/ ) {
        $self->error($error);
    }
}

=method stringify
=cut
sub stringify { $_[0]->error }

1;
