package Resque::Plugin;
use Moose::Role;

# ABSTRACT: Role for resque plugin's.

has resque_roles => ( is => 'ro', isa => 'ArrayRef', lazy => 1, default => sub{[]} );
has worker_roles => ( is => 'ro', isa => 'ArrayRef', lazy => 1, default => sub{[]} );
has job_roles    => ( is => 'ro', isa => 'ArrayRef', lazy => 1, default => sub{[]} );

1;
