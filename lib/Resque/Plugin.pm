package Resque::Plugin;
# ABSTRACT: Syntactic sugar for Resque plugin's

use Moose();
use Moose::Exporter;

Moose::Exporter->setup_import_methods(
    with_meta => ['add_to'],
    also      => 'Moose',
);

=method add_to

Role applier for Resque, Resque::Worker and Resque::Job.

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
