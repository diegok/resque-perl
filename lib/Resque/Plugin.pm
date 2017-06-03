package Resque::Plugin;
# ABSTRACT: Syntactic sugar for Resque plugin's

use Moose();
use Moose::Exporter;

Moose::Exporter->setup_import_methods(
    with_meta => ['add_to'],
    also      => 'Moose',
);

=head1 SYNOPSIS

Just initialize your Resque instance adding plugins like:

	my $resque = Resque->new( plugins => ['Duck'] );

Your plugin will define which roles will be applied to which objects:

    package Resque::Plugin::Duck;
    use Resque::Plugin;

	add_to job => 'Duck::Role'; # add this role to Resque::Job objects

Then, this role will be applied to any new job created by the Resque system:

    package Resque::Plugin::Duck::Role;
    use Moose::Role;

	has steps => ( is => 'ro', isa => 'Num', default => 0,
		traits  => ['Counter'],
		handles => { add_step => 'inc' },
	);

    sub walk { shift->add_step .' steps' }
    sub talk { 'cuac!' }

Now your jobs can walk and talk like a good duck!. A very silly example, I know:

	my $job = $resque->worker->reserve;
	say $job->walk for 1..3;
	say $job->talk;

=head1 DESCRIPTION

A Resque::Plugin allows to add moose roles to any of Resque, Resque::Worker and Resque::Job
created during the lifetime of a given Resque instance. This means you can add, replace
or augment any method of those objects.

You will define which roles will be applied to each of those objects by using the add_to() method.

Please note that you can also make use of L<Resque::WorkerClass> if what you want is to handle jobs
with Moose classes and share some roles among them.

=cut

=head1 EXPORTED FUNCTIONS

=method add_to

Role applier helper for Resque, Resque::Worker and Resque::Job.

    package Resque::Plugin::Duck;
    use Resque::Plugin;

    add_to resque => 'Duck::Talk';
    add_to worker => ['Duck::Talk', '+Resque::Plugin::Duck::Walk'];
    add_to job    => qw/ Duck::Talk Duck::Walk /;

=cut
sub add_to {
    my ( $meta, $to, @options ) = @_;
    return unless @options;

    die "Can't add roles to '$to'. Only 'resque', 'worker' and 'job' are allowed!\n" 
        unless $to =~ /^(?:resque|worker|job)$/;

    $meta->add_attribute( "${to}_roles", is => 'ro', default => _build_default( @options ) );
}

sub _build_default {
    my $opt = @_ == 1 && ref $_[0] ? $_[0] : [@_];
    return sub { $opt };
}

1;
