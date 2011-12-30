package Resque::Encoder;
use Any::Moose 'Role';
use JSON;

# ABSTRACT: Any::Moose role for encoding Resque structures

=attr encoder
  JSON encoder by default.
=cut
has encoder => ( is => 'ro', default => sub { JSON->new->utf8 } );

1;
