package Resque::Encoder;
use Moose::Role;
use JSON;

# ABSTRACT: Moose role for encoding Resque structures

=attr encoder

JSON encoder by default.

=cut
has encoder => ( is => 'ro', default => sub { JSON->new->utf8 } );

1;
