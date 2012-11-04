use Test::More;
use Resque;
use lib 't/lib';
use Test::SpawnRedisServer;

my ($c, $server) = redis();
END { $c->() if $c }

{
    my $r = new_with_plugins('Empty');
    isa_ok( $r->plugins->[0], 'Resque::Plugin::Empty' );
}

done_testing();

sub new_with_plugins {
    my $plugins = join ', ', @_;

    ok ( my $r = Resque->new( 
            redis     => $server, 
            namespace => 'test_resque',
            plugins   => [@_]
    ), "Building object for test server $server with plugins: $plugins" );

    ok ( $r->redis, 'Has redis object' );
    ok ( $r->redis->ping, 'Redis object is alive' );

    $r->flush_namespace;

    $r;
}
