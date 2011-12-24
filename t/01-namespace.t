use Test::More;

BEGIN {
	use_ok( 'Resque' );
}

{
    ok ( my $r = Resque->new(), 'Build default object' );
    test_namespace( $r => 'resque' );
}
{
    ok ( my $r = Resque->new( namespace => 'perl' ), 'Build default object' );
    test_namespace( $r => 'perl' );
}

sub test_namespace {
    my ( $r, $namespace ) = @_;
    is ( $r->namespace, $namespace, "Default namespace is $namespace" );
    is ( $r->key( queues => 'test' ), "$namespace:queues:test", 'Key generator use namespace' );
}

done_testing();
