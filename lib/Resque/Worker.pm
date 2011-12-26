package Resque::Worker;
use Any::Moose;
with 'Resque::Encoder';

use Resque::Stat;
use POSIX ":sys_wait_h";
use Sys::Hostname;
use Unix::PID;
use Scalar::Util qw(blessed weaken);
use List::MoreUtils qw{ uniq any };
use DateTime;
use Try::Tiny;

# ABSTRACT: Does the hard work of babysitting Resque::Job's

use overload 
    '""' => \&_string,
    '==' => \&is_equal,
    'eq' => \&is_equal;

has 'resque' => (
    is       => 'ro',
    required => 1,
    handles  => [qw/ redis key /]
);

has queues => (
    is      => 'rw',
    isa     => 'ArrayRef',
    lazy    => 1,
    default => sub {[]}
);

has stat => (
    is      => 'ro',
    lazy    => 1,
    default => sub { Resque::Stat->new( resque => $_[0]->resque ) }
);

has id => ( is => 'rw', lazy => 1, default => sub { $_[0]->stringify } );
sub _string { $_[0]->id } # can't point overload to a mo[o|u]se attribute :-(

has verbose   => ( is => 'rw', default => sub {0} );
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
has interval => ( is => 'rw', default => sub{5} );

=method pause
  Stop processing jobs after the current one has completed (if we're
  currently running one).
=cut
sub pause           { $_[0]->paused(1) }

=method pause
  Start processing jobs again after a pause
=cut
sub unpause         { $_[0]->paused(0) }

# Schedule this worker for shutdown. Will finish processing the
# current job.
sub shutdown_please { 
    print "Shutting down...\n";
    $_[0]->shutdown(1); 
}

# Kill the child and shutdown immediately.
sub shutdown_now    { $_[0]->shutdown_please && $_[0]->kill_child }

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
            sleep $self->interval;
        }
    }
    $self->unregister_worker;
}

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
    }
    else {
        $self->procline( sprintf( "Processing %s since %s", $job->queue, $timestamp ) );
        $self->perform($job);
        exit(0) unless $self->cant_fork;
    }

    $self->done_working;
    $self->child(0);
}

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

# Kills the forked child immediately, without remorse. The job it
# is processing will not be completed.
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

sub add_queue {
    my $self = shift;
    return unless @_;
    $self->queues( [ uniq( @{$self->queues}, @_ ) ] );
}

sub del_queue {
    my ( $self, $queue ) = @_;
    return unless $queue;
    
    return 
    @{$self->queues} 
           -
    @{$self->queues( [ grep {$_} map { $_ eq $queue ? undef : $_ } @{$self->queues} ] )};
}

sub next_queue {
    my $self = shift;
    if ( @{$self->queues} > 1 ) {
        push @{$self->queues}, shift @{$self->queues};
    }
    return $self->queues->[-1];
}

sub reserve {
    my $self = shift;
    my $count = 0;
    while ( my $queue = $self->next_queue ) {
        if ( my $job = $self->resque->pop($queue) ) {
            return $job;
        }
        return if ++$count == @{$self->queues};
    }
}

sub working_on {
    my ( $self, $job ) = @_;
    $self->redis->set( 
        $self->key( worker => $self->id ), 
        $self->encoder->encode({
            queue   => $job->queue,
            run_at  => DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z"),
            payload => $job->as_hashref
        })
    );
    $job->worker($self);
}

sub done_working {
    my $self = shift;
    $self->processed(1);
    $self->redis->del( $self->key( worker => $self->id ) );
}

# What time did this worker start? Returns an instance of `Time`
sub started {
    my $self = shift;
    $self->redis->get( $self->key( worker => $self->id => 'started' ) );
    #TODO -> parse datetime and return DT object.
}

# Tell Redis we've started
sub set_started {
    my $self = shift;
    $self->redis->set( $self->key( worker => $self->id => 'started' ), DateTime->now->strftime('%Y-%m-%d %H:%M:%S %Z') );
}

# Returns a hash explaining the Job we're currently processing, if any.
sub processing {
    my $self = shift;
    eval { $self->encoder->decode( $self->redis->get( $self->key( worker => $self->id ) ) ) } || {};
}

# Boolean - true if working, false if not
sub is_working {
    my $self = shift;
    $self->state eq 'working';
}

# Boolean - true if idle, false if not
sub is_idle {
    my $self = shift;
    $self->state eq 'idle';
}

# Returns a symbol representing the current worker state,
# which can be either :working or :idle
sub state {
    my $self = shift;
    $self->redis->exists( $self->key( worker => $self->id ) ) ? 'working' : 'idle';
}

# The string representation is the same as the id for this worker
# instance. Can be used with `Worker.find`.
sub stringify {
    my $self = shift;
    join ':', hostname, $$, join( ',', @{$self->queues} );
}


# Is this worker the same as another worker?
sub is_equal {
    my ($self, $other) = @_;
    $self->id eq $other->id;
}

# Given a string, sets the procline ($0) and logs.
# Procline is always in the format of:
#   resque-VERSION: STRING
sub procline {
    my $self = shift;
    if ( my $str = shift ) {
        $0 = sprintf( "resque-%s: %s", $Resque::VERSION || 'devel', $str );
    }
    $0;
}

sub startup {
    my $self = shift;
    $0 = 'resque: Starting';

    $self->register_signal_handlers;
    $self->prune_dead_workers;
    #run_hook: before_first_fork
    $self->register_worker;
}

# Registers the various signal handlers a worker responds to.
#
# TERM: Shutdown immediately, stop processing jobs.
#  INT: Shutdown immediately, stop processing jobs.
# QUIT: Shutdown after the current job has finished processing.
# USR1: Kill the forked child immediately, continue processing jobs.
# USR2: Don't process any new jobs
# CONT: Start processing jobs again after a USR2
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

# Model methods

sub all {
    my $self = shift;
    my @w = grep {$_} map { $self->find($_) } $self->redis->smembers( $self->key('workers') );
    return wantarray ? @w : \@w;
}

sub exists {
    my ($self, $worker_id) = @_;
    $self->redis->sismember( $self->key( 'workers' ), $worker_id );
}

# Returns a single worker object. Accepts a string id.
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

# Looks for any workers which should be running on this server
# and, if they're not, removes them from Redis.
#
# This is a form of garbage collection. If a server is killed by a
# hard shutdown, power failure, or something else beyond our
# control, the Resque workers will not die gracefully and therefore
# will leave stale state information in Redis.
#
# By checking the current Redis state against the actual
# environment, we can determine if Redis is old and clean it up a bit.
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

# Registers ourself as a worker. Useful when entering the worker
# lifecycle on startup.
sub register_worker {
    my $self = shift;
    $self->redis->sadd( $self->key( 'workers'), $self->id );
    $self->set_started;
}

# Unregisters ourself as a worker. Useful when shutting down.
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

# Returns an Array of string pids of all the other workers on this
# machine. Useful when pruning dead workers on startup.
sub worker_pids {
    my @pids = Unix::PID->new->getpidof('resque.pl'); #FIXME -> is this the command I need to look at?
    return wantarray ? @pids : \@pids;
}

sub log {
    my $self = shift;
    return unless $self->verbose;
    print shift, "\n";
}

sub processed {
    my $self = shift;
    if (shift) {
        $self->stat->incr('processed');
        $self->stat->incr("processed:$self");
    }
    $self->stat->get("processed:$self");
}

# How many failed jobs has this worker seen? Returns an int.
# Tells Redis we've failed a job.
sub failed {
    my $self = shift;
    if (shift) {
        $self->stat->incr('failed');
        $self->stat->incr("failed:$self");
    }
    $self->stat->get("failed:$self");
}

__PACKAGE__->meta->make_immutable();

