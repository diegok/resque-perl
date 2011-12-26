package Resque::Encoder;
use Any::Moose 'Role';
use JSON;

=attr encoder
  JSON encoder by default.
=cut
has encoder => ( is => 'ro', default => sub { JSON->new->utf8 } );

1;
