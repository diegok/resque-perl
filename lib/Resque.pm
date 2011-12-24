package Resque;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';

# ABSTRACT: Redis-backed library for creating background jobs, placing them on multiple queues, and processing them later.

use Redis;
use Resque::Job;

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

=attr redis
=cut
has redis => (
    is      => 'ro',
    lazy    => 1,
    coerce  => 1,
    isa     => 'Redis',
    default => sub { Redis->new }
);
coerce Redis => from 'Str' 
    => via { Redis->new( server => $_ ) };

=attr namespace
=cut
has namespace => ( is => 'rw', default => sub { 'resque' } );


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
    $job = $self->new_job($job) unless ref $job eq 'Resque::Job'; 
    $self->redis->rpush( $self->key( queue => $queue ), $job->encode );
}

=method pop
  Pops a job off a queue. Queue name should be a string.
 
  Returns a Resque::Job object.
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
=cut
sub size {
    my ( $self, $queue ) = @_;
    $self->redis->llen( $self->key( queue => $queue ) );
}

=method peek
  Returns an array of jobs currently queued. 

  First argument is queue name and an optional secound and third are
  start and count values that can be used for pagination.
  start is the item to begin, count is how many items to return.

  Passing a negative count argument will set a stop value instead
  of count. So, passing -1 will return full list, -2 all but last
  element and so on.
 
  To get the 3rd page of a 30 item, paginatied list one would use:
    $resque->peek('my_queue', 59, 30)
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

=method list_range
  Does the dirty work of fetching a range of items from a Redis list.
=cut
sub list_range {
    my ( $self, $key, $start, $count ) = @_;
    my $stop = $count > 0 ? $start + $count - 1 : $count;
    my @items =  $self->redis->lrange($key, $start, $stop);
    return \@items;
}

=method queues
  Returns an array of all known Resque queues.
=cut
sub queues {
    my $self = shift;
    my @queues = $self->redis->smembers( $self->key('queues') );
    return wantarray ? @queues : \@queues;
}

=method remove_queue
  Given a queue name, completely deletes the queue.
=cut
sub remove_queue {
    my ( $self, $queue ) = @_;
    $self->redis->srem( $self->key('queues'), $queue );
    $self->redis->del( $self->key( queue => $queue ) );
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
    
    $rescue->mass_dequeue({ 
        queue => 'test', 
        class => 'UpdateGraph' 
    });
    
  Whereas specifying args will only remove the 2nd job:
    
    $rescue->mass_dequeue({ 
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
    if ( exists $target->{args} ) {
        $removed += $self->redis->lrem( $queue, 0, $self->new_job($target)->encode );
    }
    else {
        for my $item ( $self->redis->lrange( $queue, 0, -1 ) ) {
            if ( $self->new_job( $item )->class eq $target->{class} ) {
                $removed += $self->redis->lrem( $queue, 0, $item );
            }
        }
    }

    $removed;
}

=method new_job
  Build a Resque::Job object on this system for the given
  hashref(see Resque::Job) or string(payload for object).
=cut
sub new_job {
    my ( $self, $job ) = @_;

    if ( $job && ref $job && ref $job eq 'HASH' ) { 
         return Resque::Job->new({ resque => $self, %$job }); 
    }
    elsif ( $job ) {
        return Resque::Job->new({ resque => $self, payload => $job });
    }
    confess "Can't build an empty Resque::Job object.";
}

=method keys
  Returns an array of all known Resque keys in Redis. Redis' KEYS operation
  is O(N) for the keyspace, so be careful - this can be slow for big databases.
=cut
sub keys {
    my $self = shift;
    my @keys = $self->redis->keys( $self->key('*') );
    return wantarray ? @keys : \@keys;
}

# Used internally to keep track of which queues we've created.
# Don't call this directly.
sub _watch_queue {
    my ( $self, $queue ) = @_;
    $self->redis->sadd( $self->key('queues'), $queue );
}

=method key
  Concatenate $self->namespace with the received array of names
  to build a redis key name for this resque instance.
=cut
sub key {
    my $self = shift;
    join( ':', $self->namespace, @_ );
}

sub flush_namespace {
    my $self = shift;
    if ( my @keys = $self->keys ) {
        return $self->redis->del( @keys ); 
    }
    return 0;
}

__PACKAGE__->meta->make_immutable();

__DATA__

  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # Returns true if the job was queued, nil if the job was rejected by a
  # before_enqueue hook.
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    enqueue_to(queue_from_class(klass), klass, *args)
  end

  # Just like `enqueue` but allows you to specify the queue you want to
  # use. Runs hooks.
  #
  # `queue` should be the String name of the queue you're targeting.
  #
  # Returns true if the job was queued, nil if the job was rejected by a
  # before_enqueue hook.
  #
  # This method is considered part of the `stable` API.
  def enqueue_to(queue, klass, *args)
    # Perform before_enqueue hooks. Don't perform enqueue if any hook returns false
    before_hooks = Plugin.before_enqueue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return nil if before_hooks.any? { |result| result == false }

    Job.create(queue, klass, *args)

    Plugin.after_enqueue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end

    return true
  end

  # This method can be used to conveniently remove a job from a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If no args are given, this method will dequeue *all* jobs matching
  # the provided class. See `Resque::Job.destroy` for more
  # information.
  #
  # Returns the number of jobs destroyed.
  #
  # Example:
  #
  #   # Removes all jobs of class `UpdateNetworkGraph`
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph)
  #
  #   # Removes all jobs of class `UpdateNetworkGraph` with matching args.
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph, 'repo:135325')
  #
  # This method is considered part of the `stable` API.
  def dequeue(klass, *args)
    # Perform before_dequeue hooks. Don't perform dequeue if any hook returns false
    before_hooks = Plugin.before_dequeue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return if before_hooks.any? { |result| result == false }

    Job.destroy(queue_from_class(klass), klass, *args)

    Plugin.after_dequeue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end
  end

  # Given a class, try to extrapolate an appropriate queue based on a
  # class instance variable or `queue` method.
  def queue_from_class(klass)
    klass.instance_variable_get(:@queue) ||
      (klass.respond_to?(:queue) and klass.queue)
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end

  # Validates if the given klass could be a valid Resque job
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If given klass is nil this method will raise a `Resque::NoClassError`
  def validate(klass, queue = nil)
    queue ||= queue_from_class(klass)

    if !queue
      raise NoQueueError.new("Jobs must be placed onto a queue.")
    end

    if klass.to_s.empty?
      raise NoClassError.new("Jobs must be given a class.")
    end
  end


  #
  # worker shortcuts
  #

  # A shortcut to Worker.all
  def workers
    Worker.all
  end

  # A shortcut to Worker.working
  def working
    Worker.working
  end

  # A shortcut to unregister_worker
  # useful for command line tool
  def remove_worker(worker_id)
    worker = Resque::Worker.find(worker_id)
    worker.unregister_worker
  end

  #
  # stats
  #

  # Returns a hash, similar to redis-rb's #info, of interesting stats.
  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.size,
      :failed    => Stat[:failed],
      :servers   => [redis_id],
      :environment  => ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development'
    }
  end

