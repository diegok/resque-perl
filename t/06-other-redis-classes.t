use Test::More;
use Test::Exception;
use Resque;
use lib 't/lib';
use Test::Redis;
use Test::RedisMoose;


{
	my $redis = Test::Redis->new;
	my $resque;
	ok($resque = Resque->new(redis => $redis), "Can create a resque object with any redis object");
    isa_ok($resque, 'Resque');
}

{
	my $redis = Test::RedisMoose->new;
	my $resque;
	ok($resque = Resque->new(redis => $redis), "Can create a resque object with any redis Moose object");
    isa_ok($resque, 'Resque');
}

{
	my $redis = {};
	my $resque;
	throws_ok {$resque = Resque->new(redis => $redis)}  qr/Attribute \(redis\) does not pass the type constraint/, "Can't create a resque object unblessed redis 'object'";
}

done_testing();

