use Test::More tests => 1;

BEGIN {
	use_ok( 'Resque' );
}

diag( "Testing Resque $Resque::VERSION, Perl $], $^X" );
