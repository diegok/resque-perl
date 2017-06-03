package Resque;
# ABSTRACT: Redis-backed library for creating background jobs, placing them on multiple queues, and processing them later.

use Moose;
use Scalar::Util 'blessed';
use Moose::Util::TypeConstraints;
use Class::Load;
use Data::Compare;
with 'Resque::Pluggable';

use Resque::Job;
use Resque::Worker;
use Resque::Failures;

sub _redis_class{ Class::Load::load_first_existing_class( 'Redis::Fast', 'Redis' ) }

=head1 SYNOPSIS

First you create a Resque instance where you configure the L<Redis> backend and then you can
start sending jobs to be done by workers:

    use Resque;

    my $r = Resque->new( redis => '127.0.0.1:6379' );

    $r->push( my_queue => {
        class => 'My::Task',
        args => [ 'Hello world!' ]
    });

Background jobs can be any perl module that implement a perform() function. The Resque::Job object is
passed as the only argument to this function:

    package My::Task;
    use strict;
    use 5.10.0;

    sub perform {
        my $job = shift;
        say $job->args->[0];
    }

    1;

Finally, you run your jobs by instancing a L<Resque::Worker> and telling it to listen to one or more
queues:

    use Resque;

    my $w = Resque->new( redis => '127.0.0.1:6379' )->worker;
    $w->add_queue('my_queue');
    $w->work;

=head1 DESCRIPTION

Resque is a Redis-backed library for creating background jobs, placing them on multiple queues,
and processing them later.

This library is a perl port of the original Ruby one: L<https://github.com/defunkt/resque>
My main goal doing this port is to use the same backend to be able to manage the system using
ruby's resque-server webapp.

As extracted from the original docs, the main features of Resque are:

Resque workers can be distributed between multiple machines, support priorities, are resilient to
memory leaks, tell you what they're doing, and expect failure.

Resque queues are persistent; support constant time, atomic push and pop (thanks to Redis); provide
visibility into their contents; and store jobs as simple JSON hashes.

The Resque frontend tells you what workers are doing, what workers are not doing, what queues you're
using, what's in those queues, provides general usage stats, and helps you track failures.

A lot more about Resque can be read on the original blog post: L<http://github.com/blog/542-introducing-resque>

=cut

=head1 ATTRIBUTES

=attr redis

Redis instance for this Resque instance. Accepts a string, L<Redis>, L<Redis::Fast> or any other object that behaves like those.

When a string is passed in, it will be used as the server argument of a new client object. When L<Redis::Fast> is available this
will be used, when not the pure perl L<Redis> client will be used instead.

=cut

subtype 'Sugar::Redis' => as 'Object';
coerce 'Sugar::Redis' => from 'Str' => via {
    _redis_class->new(
        server    => $_,
        reconnect => 60,
        every     => 250,
        encoding  => undef
    )
};

has redis => (
    is      => 'ro',
    lazy    => 1,
    coerce  => 1,
    isa     => 'Sugar::Redis',
    default => sub { _redis_class->new }
);

=attr namespace

This is useful to run multiple queue systems with the same Redis backend.

By default 'resque' is used.

=cut
has namespace => ( is => 'rw', default => sub { 'resque' } );

=attr failures

Failures handler. See L<Resque::Failures>.

=cut
has failures => (
    is   => 'rw',
    lazy => 1,
    default => sub { Resque::Failures->new( resque => $_[0] ) },
    handles => [qw/ throw /]
);

=method worker

Returns a new L<Resque::Worker> on this resque instance.
It can have plugin/roles applied. See L<Resque::Pluggable>.

    my $worker = $r->worker();

=cut
sub worker {
    my $self = shift;
    $self->worker_class->new( resque => $self );
}

=head1 Queue manipulation

=method push

Pushes a job onto a queue. Queue name should be a string and the
item should be a Resque::Job object or a hashref containing:

 class - The String name of the job class to run.
  args - Any arrayref of arguments to pass the job.

Returns redis response.

Example

    $resque->push( archive => { class => 'Archive', args => [ 35, 'tar' ] } )

=cut
sub push {
    my ( $self, $queue, $job ) = @_;
    confess "Can't push an empty job." unless $job;
    $self->_watch_queue($queue);
    $job = $self->new_job($job) unless blessed $job && $job->isa('Resque::Job');
    $self->redis->rpush( $self->key( queue => $queue ), $job->encode );
}

=method pop

Pops a job off a queue. Queue name should be a string.

Returns a Resque::Job object.

    my $resque_job = $r->pop( 'queue_name' );

=cut
sub pop {
    my ( $self, $queue ) = @_;
    my $payload = $self->redis->lpop($self->key( queue => $queue ));
    return unless $payload;

    $self->new_job({
        payload => $payload,
        queue   => $queue
    });
}

=method size

Returns the size of a queue.
Queue name should be a string.

    my $size = $r->size();

=cut
sub size {
    my ( $self, $queue ) = @_;
    $self->redis->llen( $self->key( queue => $queue ) );
}

=method peek

Returns an array of jobs currently queued, or an arrayref in scalar context.

First argument is queue name and an optional secound and third are
start and count values that can be used for pagination.
start is the item to begin, count is how many items to return.

Passing a negative count argument will set a stop value instead
of count. So, passing -1 will return full list, -2 all but last
element and so on.

To get the 3rd page of a 30 item, paginatied list one would use:

    my @jobs = $resque->peek('my_queue', 59, 30)

=cut
sub peek {
    my ( $self, $queue, $start, $count ) = @_;
    my $jobs = $self->list_range(
        $self->key( queue => $queue ),
        $start || 0, $count || 1
    );
    $_ = $self->new_job({ queue => $queue, payload => $_ }) for @$jobs;
    return wantarray ? @$jobs : $jobs;
}

=method queues

Returns an array of all known Resque queues, or an arrayref in scalar context.

    my @queues = $r->queues();

=cut
sub queues {
    my $self = shift;
    my @queues = $self->redis->smembers( $self->key('queues') );
    return wantarray ? @queues : \@queues;
}

=method remove_queue

Given a queue name, completely deletes the queue.

    $r->remove_queue( 'my_queue' );

=cut
sub remove_queue {
    my ( $self, $queue ) = @_;
    $self->redis->srem( $self->key('queues'), $queue );
    $self->redis->del( $self->key( queue => $queue ) );
}

=method create_queue

Given a queue name, creates an empty queue.

    $r->create_queue( 'my_queue' );

=cut
sub create_queue {
    my ( $self, $queue ) = @_;
    $self->_watch_queue( $queue );
}

=method mass_dequeue

Removes all matching jobs from a queue. Expects a hashref
with queue name, a class name, and, optionally, args.

Returns the number of jobs destroyed.

If no args are provided, it will remove all jobs of the class
provided.

That is, for these two jobs:

  { 'class' => 'UpdateGraph', 'args' => ['perl'] }
  { 'class' => 'UpdateGraph', 'args' => ['ruby'] }

The following call will remove both:

    my $num_removed = $rescue->mass_dequeue({
        queue => 'test',
        class => 'UpdateGraph'
    });

Whereas specifying args will only remove the 2nd job:

    my $num_removed = $rescue->mass_dequeue({
        queue => 'test',
        class => 'UpdateGraph',
        args  => ['ruby']
    });

Using this method without args can be potentially very slow and
memory intensive, depending on the size of your queue, as it loads
all jobs into an array before processing.

=cut
sub mass_dequeue {
    my ( $self, $target ) = @_;
    confess("Can't mass_dequeue() without queue and class names.")
        unless $target
        and $target->{queue}
        and $target->{class};

    my $queue = $self->key( queue => $target->{queue} );
    my $removed = 0;
    for my $item ( $self->redis->lrange( $queue, 0, -1 ) ) {
        my $job_item = $self->new_job( $item );
        if ( $job_item->class eq $target->{class} ) {
            if ( exists $target->{args} ) {
                next unless Compare( $job_item->args, $target->{args} );
            }

            $removed += $self->redis->lrem( $queue, 0, $item );
        }
    }

    $removed;
}

=method new_job

Build a L<Resque::Job> object on this system for the given
hashref or string(payload for object).

L<Resque::Job> class can be extended thru roles/plugins.
See L<Resque::Pluggable>.

    $r->new_job( $job_or_job_hashref );

=cut
sub new_job {
    my ( $self, $job ) = @_;

    if ( $job && ref $job && ref $job eq 'HASH' ) {
         return $self->job_class->new({ resque => $self, %$job });
    }
    elsif ( $job ) {
        return $self->job_class->new({ resque => $self, payload => $job });
    }
    confess "Can't build an empty Resque::Job object.";
}

=head1 HELPER METHODS

=method key

Concatenate $self->namespace with the received array of names
to build a redis key name for this resque instance.

=cut
sub key {
    my $self = shift;
    join( ':', $self->namespace, @_ );
}

=method keys

Returns an array of all known Resque keys in Redis, or an arrayref in scalar context.
Redis' KEYS operation is O(N) for the keyspace, so be careful this can be slow for
big databases.

=cut
sub keys {
    my $self = shift;
    my @keys = $self->redis->keys( $self->key('*') );
    return wantarray ? @keys : \@keys;
}

=method flush_namespace

This method will delete every trace of this Resque system on
the redis() backend.

    $r->flush_namespace();

=cut
sub flush_namespace {
    my $self = shift;
    if ( my @keys = $self->keys ) {
        return $self->redis->del( @keys );
    }
    return 0;
}

=method list_range

Does the dirty work of fetching a range of items from a Redis list.

    my $items_ref = $r->list_range( $key, $stat, $count );

=cut
sub list_range {
    my ( $self, $key, $start, $count ) = @_;
    my $stop = $count > 0 ? $start + $count - 1 : $count;
    my @items =  $self->redis->lrange( $key, $start, $stop );
    return \@items;
}

# Used internally to keep track of which queues we've created.
# Don't call this directly.
sub _watch_queue {
    my ( $self, $queue ) = @_;
    $self->redis->sadd( $self->key('queues'), $queue );
}

__PACKAGE__->meta->make_immutable();

=head1 BUGS

As in any piece of software there might be bugs around.
If you found one, please report it on RT or at the github repo:

L<https://github.com/diegok/resque-perl>

Pull requests are also very welcomed, but please include tests demostrating
what you've fixed.

=head1 TODO

=for :list
* More tests on worker fork and signal handling.

=head1 SEE ALSO

=for :list
* L<Gearman>
* L<TheSchwartz>
* L<Queue::Q4M>

=cut
