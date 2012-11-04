package Resque::Pluggable;
use Moose::Role;

# ABSTRACT: Role to load Resque plugin's and and apply roles.

use namespace::autoclean;
use Class::Load qw(load_class);
use Moose::Util qw(apply_all_roles);

=attr plugins
List of plugins to be loaded into this L<Resque> system.
=cut
has plugins => (
    is      => 'rw',
    isa     => 'ArrayRef',
    lazy    => 1,
    default => sub {[]}
);

=attr worker_class
Worker class to be used for worker attribute.
This is L<Resque::Worker> with all plugin/roles applied to it.
=cut
has worker_class => (
    is   => 'ro',
    lazy => 1,
    default => sub { 'Resque::Worker' }
);

=attr job_class
Job class to be used by L<Resque::new_job>.
This is L<Resque::Job> with all plugin/roles applied to it.
=cut
has job_class => (
    is   => 'ro',
    lazy => 1,
    default => sub { 'Resque::Job' }
);

sub BUILD {
    my $self = shift;
    $self->_load_plugins;
    if ( my @r = $self->roles_for('resque') ) {
        apply_all_roles( $self, @r );
    }
}

# Build anon class based on the given one with optional roles applied 
sub _class_with_roles {
    my ( $self, $class ) = ( shift, shift );
    return $class unless @_;

    my $meta = Moose::Meta::Class->create_anon_class(
        superclasses => [$class],
        roles        => [@_] 
    );

    $meta->make_immutable;
    return $meta->name;
}

sub _load_plugins {
    my $self = shift;

    my @plugins;
    for my $name ( @{$self->plugins} ) {
        $name = _expand_plugin_namespace($name);
        load_class($name);
        my $plugin = $name->new;
        confess "$name doesn't do Resque::Plugin" unless $plugin->does('Resque::Plugin');
        push @plugins, $plugin;
    }
    $self->plugins(\@plugins);
}

sub _expand_plugin_namespace {
    my $name = pop;
    $name = $name =~ /^\+(.+)$/ ? $1 : "Resque::Plugin::$name";
    return $name;
}

sub roles_for {
    my ( $self, $obj_name ) = @_;
    my $method_name = "${obj_name}_roles";

    my @roles;
    for my $plugin ( @{$self->plugins} ) {
        push @roles, @{$plugin->$method_name};
    }
    return @roles;
}

1;
