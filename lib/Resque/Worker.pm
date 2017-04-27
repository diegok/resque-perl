package Resque::Worker;
# ABSTRACT: Does the hard work of babysitting Resque::Job's

use Moose;
with 'Resque::Encoder';

use FindBin; # so it will work after playing around $0
use Resque::Stat;
use POSIX ":sys_wait_h";
use Sys::Hostname;
use Scalar::Util qw(blessed weaken);
use List::MoreUtils qw(uniq any);
use Time::HiRes qw(sleep);
use DateTime;
use Try::Tiny;

use overload
    '""' => \&_string,
    '==' => \&_is_equal,
    'eq' => \&_is_equal;

=attr resque

The L<Resque> object running this worker.

=cut
has 'resque' => (
    is       => 'ro',
    required => 1,
    handles  => [qw/ redis key /]
);

=attr queues

Queues this worker should fetch jobs from.

=cut
has queues => (
    is      => 'rw',
    isa     => 'ArrayRef',
    lazy    => 1,
    default => sub {[]}
);

=attr stat

See L<Resque::Stat>.

=cut
has stat => (
    is      => 'ro',
    lazy    => 1,
    default => sub { Resque::Stat->new( resque => $_[0]->resque ) }
);

=attr id

Unique identifier for the running worker.
Used to set process status all around.

The worker stringify to this attribute.

=cut
has id => ( is => 'rw', lazy => 1, default => sub { $_[0]->_stringify } );
sub _string { $_[0]->id } # can't point overload to a mo[o|u]se attribute :-(

=attr verbose

Set to a true value to make this worker report what's doing while
on work().

=cut
has verbose   => ( is => 'rw', default => sub {0} );

=attr cant_fork

Set it to a true value to stop this worker from fork jobs.

By default, the worker will fork the job out and control the
children process. This make the worker more resilient to
memory leaks.

=cut
has cant_fork => ( is => 'rw', default => sub {0} );

=attr child

PID of current running child.

=cut
has child    => ( is => 'rw' );

=attr shutdown

When true, this worker will shutdown after finishing current job.

=cut
has shutdown => ( is => 'rw', default => sub{0} );

=attr paused

When true, this worker won't proccess more jobs till false.

=cut
has paused   => ( is => 'rw', default => sub{0} );

=attr interval

Float representing the polling frequency. The default is 5 seconds, but for a semi-active app you may want to use a smaller value.

=cut
has interval => ( is => 'rw', default => sub{5} );

=method pause

Stop processing jobs after the current one has completed (if we're
currently running one).
 
$worker->pause();

=cut
sub pause           { $_[0]->paused(1) }

=method unpause

Start processing jobs again after a pause

$worker->unpause();

=cut
sub unpause         { $_[0]->paused(0) }

=method shutdown_please

Schedule this worker for shutdown. Will finish processing the
current job.

$worker->shutdown_please();

=cut
sub shutdown_please {
    print "Shutting down...\n";
    $_[0]->shutdown(1);
}

=method shutdown_now

Kill the child and shutdown immediately.

$worker->shutdown_now();

=cut
sub shutdown_now    { $_[0]->shutdown_please && $_[0]->kill_child }

=method work

Calling this method will make this worker start pulling & running jobs
from queues().

This is the main wheel and will run while shutdown() is false.

$worker->work();

=cut
sub work {
    my $self = shift;
    $self->startup;
    while ( ! $self->shutdown ) {
        if ( !$self->paused && ( my $job = $self->reserve ) ) {
            $self->log("Got job $job");
            $self->work_tick($job);
        }
        elsif( $self->interval ) {
            my $status = $self->paused ? "Paused" : 'Waiting for ' . join( ', ', @{$self->queues} );
            $self->procline( $status );
            $self->log( $status );
            sleep( $self->interval );
        }
    }
    $self->unregister_worker;
}

=method work_tick

Perform() one job and wait till it finish.

$worker->work_tick();

=cut
sub work_tick {
    my ($self, $job) = @_;

    $self->working_on($job);
    my $timestamp = DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z");

    if ( !$self->cant_fork && ( my $pid = fork ) ) {
        $self->procline( "Forked $pid at $timestamp" );
        $self->child($pid);
        $self->log( "Waiting for $pid" );
        #while ( ! waitpid( $pid, WNOHANG ) ) { } # non-blocking has sense?
        waitpid( $pid, 0 );
        $self->log( "Forked job($pid) exited with status $?" );

        if ($?) {
            $job->fail("Exited with status $?");
            $self->failed(1);
        }
    }
    else {
        undef $SIG{TERM};
        undef $SIG{INT};
        undef $SIG{QUIT};

        $self->procline( sprintf( "Processing %s since %s", $job->queue, $timestamp ) );
        $self->perform($job);
        exit(0) unless $self->cant_fork;
    }

    $self->done_working;
    $self->child(0);
}


=method perform

Call perform() on the given Resque::Job capturing and reporting
any exception.

$worker->perform( $job );

=cut
sub perform {
    my ( $self, $job ) = @_;
    my $ret;
    try {
        $ret = $job->perform;
        $self->log( sprintf( "done: %s", $job->stringify ) );
    }
    catch {
        $self->log( sprintf( "%s failed: %s", $job->stringify, $_ ) );
        $job->fail($_);
        $self->failed(1);
    };
    $ret;
}

=method kill_child

Kills the forked child immediately, without remorse. The job it
is processing will not be completed.

$worker->kill_child();

=cut
sub kill_child {
    my $self = shift;
    return unless $self->child;

    if ( kill 0, $self->child ) {
        $self->log( "Killing my child: " . $self->child );
        kill 9, $self->child;
    }
    else {
        $self->log( "Child " . $self->child . " not found, shutting down." );
        $self->shutdown_please;
    }
}

=method add_queue

Add a queue this worker should listen to.

$worker->add_queue( "queuename" );

=cut
sub add_queue {
    my $self = shift;
    return unless @_;
    $self->queues( [ uniq( @{$self->queues}, @_ ) ] );
}

=method del_queue

Stop listening to the given queue.

$worker->del_queue( "queuename" );

=cut
sub del_queue {
    my ( $self, $queue ) = @_;
    return unless $queue;

    return
    @{$self->queues}
           -
    @{$self->queues( [ grep {$_} map { $_ eq $queue ? undef : $_ } @{$self->queues} ] )};
}

=method reserve

Pull the next job to be precessed.

my $job = $worker->reserve();

=cut
sub reserve {
    my $self = shift;
    my $count = 0;
    for my $queue ( @{$self->queues} ) {
        if ( my $job = $self->resque->pop($queue) ) {
            return $job;
        }
        return if ++$count == @{$self->queues};
    }
}

=method working_on

Set worker and working status on the given L<Resque::Job>.

$job->working_on( $resque_job );

=cut
sub working_on {
    my ( $self, $job ) = @_;
    $self->redis->set(
        $self->key( worker => $self->id ),
        $self->encoder->encode({
            queue   => $job->queue,
            run_at  => DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z"),
            payload => $job->payload
        })
    );
    $job->worker($self);
}

=method done_working

Inform the backend this worker has done its current job

$job->done_working();

=cut
sub done_working {
    my $self = shift;
    $self->processed(1);
    $self->redis->del( $self->key( worker => $self->id ) );
}

=method started

What time did this worker start?
Returns an instance of DateTime.

my $datetime = $worker->started();

=cut
sub started {
    my $self = shift;
    _parsedate( $self->redis->get( $self->key( worker => $self->id => 'started' ) ) );
}

sub _parsedate {
    my $str = pop;
    my ( $year, $month, $day, $hour, $minute, $secs, $tz ) = $str =~ m|^(\d+)[-/](\d+)[-/](\d+) (\d+):(\d+):(\d+) (.+)$|;
    DateTime->new( day => $day, month => $month, year => $year, hour => $hour, minute => $minute, second => $secs, time_zone => $tz );
}

=method set_started

Tell Redis we've started

$worker->set_started();

=cut
sub set_started {
    my $self = shift;
    $self->redis->set( $self->key( worker => $self->id => 'started' ), DateTime->now->strftime('%Y-%m-%d %H:%M:%S %Z') );
}

=method processing

Returns a hash explaining the Job we're currently processing, if any.

$worker->processing();

=cut
sub processing {
    my $self = shift;
    eval { $self->encoder->decode( $self->redis->get( $self->key( worker => $self->id ) ) ) } || {};
}

=method processing_started

What time did this worker started to work on current job?
Returns an instance of DateTime or undef when it's not working.

my $datetime = $worker->processing_started();

=cut
sub processing_started {
    my $self = shift;
    my $run_at = $self->processing->{run_at} || return;
    _parsedate($run_at);
}

=method state

Returns a string representing the current worker state,
which can be either working or idle

my $state = $worker->state();

=cut
sub state {
    my $self = shift;
    $self->redis->exists( $self->key( worker => $self->id ) ) ? 'working' : 'idle';
}

=method is_working

Boolean - true if working, false if not

my $working = $worker->is_working();

=cut
sub is_working {
    my $self = shift;
    $self->state eq 'working';
}

=method is_idle

Boolean - true if idle, false if not

my $idle = $worker->is_idle();

=cut
sub is_idle {
    my $self = shift;
    $self->state eq 'idle';
}

sub _stringify {
    my $self = shift;
    join ':', hostname, $$, join( ',', @{$self->queues} );
}

# Is this worker the same as another worker?
sub _is_equal {
    my ($self, $other) = @_;
    $self->id eq $other->id;
}

=method procline

Given a string, sets the procline ($0) and logs.
Procline is always in the format of:
    resque-VERSION: STRING

$worker->procline( "string" );

=cut
sub procline {
    my $self = shift;
    if ( my $str = shift ) {
        $0 = sprintf( "resque-%s: %s", $Resque::VERSION || 'devel', $str );
    }
    $0;
}

=method startup

Helper method called by work() to:

  1. register_signal_handlers()
  2. prune_dead_workers();
  3. register_worker();

$worker->startup();

=cut
sub startup {
    my $self = shift;
    $0 = 'resque: Starting';

    $self->register_signal_handlers;
    $self->prune_dead_workers;
    #run_hook: before_first_fork
    $self->register_worker;
}

=method register_signal_handlers

Registers the various signal handlers a worker responds to.

 TERM: Shutdown immediately, stop processing jobs.
  INT: Shutdown immediately, stop processing jobs.
 QUIT: Shutdown after the current job has finished processing.
 USR1: Kill the forked child immediately, continue processing jobs.
 USR2: Don't process any new jobs
 CONT: Start processing jobs again after a USR2

$worker->register_signal_handlers();

=cut
sub register_signal_handlers {
    my $self = shift;
    weaken $self;
    $SIG{TERM} = sub { $self->shutdown_now };
    $SIG{INT}  = sub { $self->shutdown_now };
    $SIG{QUIT} = sub { $self->shutdown_please };
    $SIG{USR1} = sub { $self->kill_child };
    $SIG{USR2} = sub { $self->pause };
    $SIG{CONT} = sub { $self->unpause };
}

=method prune_dead_workers

Looks for any workers which should be running on this server
and, if they're not, removes them from Redis.

This is a form of garbage collection. If a server is killed by a
hard shutdown, power failure, or something else beyond our
control, the Resque workers will not die gracefully and therefore
will leave stale state information in Redis.

By checking the current Redis state against the actual
environment, we can determine if Redis is old and clean it up a bit.

$worker->prune_dead_worker();

=cut
sub prune_dead_workers {
    my $self = shift;
    my @all_workers   = $self->all;
    my @known_workers = $self->worker_pids if @all_workers;
    for my $worker (@all_workers) {
        my ($host, $pid, $queues) = split( ':', $worker->id );
        next unless $host eq hostname;
        next if any { $_ eq $pid } @known_workers;
        $self->log( "Pruning dead worker: $worker" );
        $worker->unregister_worker;
    }
}

=method register_worker

Registers ourself as a worker. Useful when entering the worker
lifecycle on startup.

$worker->register_worker();

=cut
sub register_worker {
    my $self = shift;
    $self->redis->sadd( $self->key( 'workers'), $self->id );
    $self->set_started;
}

=method unregister_worker

Unregisters ourself as a worker. Useful when shutting down.

$worker->unregister_worker();

=cut
sub unregister_worker {
    my $self = shift;

    # If we're still processing a job, make sure it gets logged as a
    # failure.
    {
        my $hr = $self->processing;
        if ( %$hr ) {
            # Ensure the proper worker is attached to this job, even if
            # it's not the precise instance that died.
            my $job = $self->resque->new_job({
                worker  => $self,
                queue   => $hr->{queue},
                payload => $hr->{payload}
            });
            $job->fail( 'Dirty exit' );
        }
    }

    $self->redis->srem( $self->key('workers'), $self->id );
    $self->redis->del( $self->key( worker => $self->id ) );
    $self->redis->del( $self->key( worker => $self->id => 'started' ) );

    $self->stat->clear("processed:$self");
    $self->stat->clear("failed:$self");
}

=method worker_pids

Returns an Array of string pids of all the other workers on this
machine. Useful when pruning dead workers on startup.

my @pids = $worker->worker_pids();

=cut
sub worker_pids {
    my $self = shift;
    my @pids;

    if($^O=~m/^(cygwin|MSWin32)$/i) {
        # $0 assignment does not work under Win32, so we'll return a list of perl PIDs instead
        @pids = map { s/^PID:\s*// && $_ }
                grep { /^PID/ }
                split( /[\r\n]/ , `tasklist /FI "IMAGENAME eq perl.exe" /FO list` );
    } else {
        my $ps_command = $^O eq 'solaris'
                    ? 'ps -A -o pid,args'
                    : 'ps -A -o pid,command';

        for ( split "\n", `$ps_command | grep resque | grep -v resque-web | grep -v grep` ) {
            if ( m/^\s*(\d+)\s(.+)$/ ) {
                push @pids, $1;
            }
        }
    }
    return wantarray ? @pids : \@pids;
}

=method log

If verbose() is true, this will print to STDERR.

$worker->log( 'message here' );

=cut
#TODO: add logger() attr to containg a logger object and if set, use that instead of print!
sub log {
    my $self = shift;
    return unless $self->verbose;
    print STDERR shift, "\n";
}

=method processed

Retrieve from L<Resque::Stat> many jobs has done this worker.
Pass a true argument to increment by one before retrieval.

my $jobs_run = $worker->processed( $boolean );

=cut
sub processed {
    my $self = shift;
    if (shift) {
        $self->stat->incr('processed');
        $self->stat->incr("processed:$self");
    }
    $self->stat->get("processed:$self");
}

=method failed

How many failed jobs has this worker seen.
Pass a true argument to increment by one before retrieval.

my $jobs_run = $worker->processed( $boolean );

=cut
sub failed {
    my $self = shift;
    if (shift) {
        $self->stat->incr('failed');
        $self->stat->incr("failed:$self");
    }
    $self->stat->get("failed:$self");
}

=method find

Returns a single worker object. Accepts a string id.

my $worker_object = $worker->find( $worker_id );

=cut
sub find {
    my ( $self, $worker_id ) = @_;
    if ( $self->exists( $worker_id ) ) {
        my @queues = split ',', (split( ':', $worker_id))[-1];
        return __PACKAGE__->new(
            resque => $self->resque,
            queues => \@queues,
            id     => $worker_id
        );
    }
}

=method all

Returns a list of all worker registered on the backend, or an
arrayref in scalar context;

my @workers = $worker->all();

=cut
sub all {
    my $self = shift;
    my @w = grep {$_} map { $self->find($_) } $self->redis->smembers( $self->key('workers') );
    return wantarray ? @w : \@w;
}

=method exists

Returns true if the given worker id exists on redis() backend.

my $exists = $worker->exists( $worker_id );
=cut
sub exists {
    my ($self, $worker_id) = @_;
    $self->redis->sismember( $self->key( 'workers' ), $worker_id );
}

__PACKAGE__->meta->make_immutable();

