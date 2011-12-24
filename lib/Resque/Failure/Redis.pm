package Resque::Failure::Redis;
use Any::Moose;
# ABSTRACT: Redis backend for worker failures

has 'worker' => ( 
    isa      => 'Resque::Worker', 
    required => 1
);

has 'job' => ( 
    isa      => 'Resque::Worker', 
    required => 1
);


1;
