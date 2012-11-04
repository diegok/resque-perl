package Resque::Plugin::Duck;
use Resque::Plugin;

add_to resque => 'Duck::Talk';
add_to worker => ['Duck::Talk', '+Resque::Plugin::Duck::Walk'];
add_to job    => qw/
    Duck::Talk
    Duck::Walk
/;

1;
