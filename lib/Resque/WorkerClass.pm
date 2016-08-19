package Resque::WorkerClass;
# ABSTRACT: Resque adaptor to create moose based jobs

use Moose ();
use Moose::Exporter;

Moose::Exporter->setup_import_methods(
    with_meta => ['perform'],
    also      => 'Moose'
);

=head1 SYNOPSIS

Resque::WorkerClass let you write Moose based job classes that will expect to be called with a single argument
that should be a hashref to be pased to the constructor. Something like this:

    use Resque;
    Resque->new->push( some_queue => {
        class => 'Task::SayHello',
        args => [{ name => 'Ambar', email => 'her@mail.com' }]
    });

Using this class, you can declare your attributes as any normal Moose class, use roles and everything moosey.
The only requirement is that you should implement a run() method that's the entry point of your background job:

    package Task::SayHello;
    use Resque::WorkingClass; # This will load Moose for you
    with 'Task::Role::Sendmail'; # a role to send emails

    has name  => is => 'ro', isa => 'Str', required => 1;
    has email => is => 'ro', isa => 'Str', required => 1;

    sub run {
        my $self = shift;

        $self->sendmail(
            to      => $self->email,
            subject => sprintf('Hello %s', $self->name)
        );
    }

    1;

=head1 DESCRIPTION

Writting resque background jobs usually requires a lot of boilerplate to be able to validate arguments,
share things as database conections, configuration or just load and configure other modules.
By using this class you can use all what we love about Moose for attribute validation, use roles to be
able to share functionality among your job clases and even use method modifiers on your jobs.

The only thing you should do to start writing moose background jobs is to use() this class instead of Moose
itself and implement the mandatory run() method that will be called on a fresh instance of you class initialized
with the job only argument (see SYNOPSIS).

Your class will get injected 3 attributes: job, resque and redis that you can use to access the job object passed
to perform(), the resque system running the job and the redis object used by the resque system.

=cut

=head1 METHODS

=method perform

This will be called by L<Resque::Worker> to perform() your job. This is the glue
implementation that will initialize an instance of your class and call your run()
method on it.

=cut
sub perform {
    my ($meta, $job) = @_;

    $meta->add_attribute( job => is => 'ro', handles => [qw/ redis resque /] )
        unless $meta->get_attribute('job');

    my $job_class = ($meta->linearized_isa)[0];
    my $instance = $job_class->new( job => $job, %{$job->args->[0]} );
    $instance->can('run') ? $instance->run
                          : die "$job_class is perfrom()ing via Resque::WorkerClass without a run() method!";
}

1;
