package Resque::Failures;
# ABSTRACT: Class for managing Resque failures

use Moose;
with 'Resque::Encoder';
use Class::Load qw(load_class);
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
        load_class('Resque::Failure::Redis');
        'Resque::Failure::Redis';
    },
    trigger => sub {
        my ( $self, $class ) = @_;
        load_class($class);
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
        $self->key('failed'), $start||0, $count||-1
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

Requeue by index number.

Failure will be updated to note retried date.

=cut
sub requeue {
    my ( $self, $index ) = @_;
    my ($item) = $self->all($index, 1);
    $item->{retried_at} = DateTime->now->strftime("%Y/%m/%d %H:%M:%S");
    $self->redis->lset(
        $self->key('failed'), $index,
        $self->encoder->encode($item)
    );
    $self->_requeue($item);
}

sub _requeue {
    my ( $self, $item, $queue ) = @_;
    $self->resque->push( $queue || $item->{queue} => {
        class => $item->{payload}{class},
        args  => $item->{payload}{args},
    });
}

=method remove

Remove failure by index number in failures queue.

Please note that, when you remove some index, all
sucesive ones will move left, so index will decrese
one. If you want to remove several ones start removing
from the rightmost one.

=cut
sub remove {
    my ( $self, $index ) = @_;
    my $id = rand(0xffffff);
    my $key = $self->key('failed');
    $self->redis->lset( $key, $index, $id);
    $self->redis->lrem( $key, 1, $id );
}

=method mass_remove

Remove and optionally requeue all or matching failed jobs. Errors that happen
after this method is fired will remind untouched.

Filters, if present, are useful to select failed jobs and should be regexes or
strings that will be matched against any of the following failed job field:

    queue: the queue where job had failed
    class: the job class
    error: the error string
    args:  a JSON representation of the job arguments

By default, all matching jobs will be deleted but the ones that
doesn't match will be placed back at the end of the failed jobs.

The behavior can be modified with the following options:

    requeue: requeue matching jobs after being removed
    queue:   force requeued jobs to be placed on this queue

Example

    # Remove and requeue all failed jobs from queue 'test_queue' of class My::Job::Class
    $resque->failures->mass_remove(
        queue   => 'test_queue',
        class   => qr/^My::Job::Class$/,
        requeue => 1
    );

=cut
sub mass_remove {
    my ( $self, %opt ) = @_;
    $opt{limit} ||= $self->count || return 0;

    for (qw/queue error class args/) { $opt{$_} = qr/$opt{$_}/ if $opt{$_} && not ref $opt{$_} }

    my $key = $self->key('failed');
    my $enc = $self->encoder;

    my ( $count, $rem ) = ( 0, 0 );
    while ( my $encoded_item = $self->redis->lpop($key) ) {
        my $item = $enc->decode($encoded_item);

        my $match = (!$opt{queue} && !$opt{error} && !$opt{class} && !$opt{args})
                 || ($opt{queue} && $item->{queue} =~ $opt{queue})
                 || ($opt{error} && $item->{error} =~ $opt{error})
                 || ($opt{class} && $item->{payload}{class} =~ $opt{class})
                 || ($opt{args}  && $enc->encode($item->{payload}{args}) =~ $opt{args});

        if ( $match ) { $rem++; $self->_requeue($item, $opt{queue}) if $opt{requeue} }
        else          { $self->redis->rpush( $key => $encoded_item ) }

        last if ++$count >= $opt{limit};
    }

    $rem;
}

__PACKAGE__->meta->make_immutable();
