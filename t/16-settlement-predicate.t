# The ack/nack coverage predicate, exercised directly (no broker): exact tag,
# multiple up to a higher tag, and the AMQP "multiple=1, delivery_tag=0 = all
# outstanding" form. A single outstanding publication cannot make a real broker
# send tag 0, which is why this is tested at the predicate level.
use strict; use warnings;
use Test::More tests => 7;
use RabbitMQ::Connection;

my $s = \&RabbitMQ::Connection::_settles;   # (our tag, frame delivery_tag, multiple)
ok( $s->(5, 5, 0), 'exact tag settles');
ok(!$s->(5, 4, 0), 'lower tag without multiple does not');
ok(!$s->(5, 6, 0), 'higher tag without multiple does not (not ours)');
ok( $s->(5, 7, 1), 'multiple with a higher tag settles');
ok( $s->(5, 5, 1), 'multiple with the same tag settles');
ok(!$s->(5, 4, 1), 'multiple with a lower tag does not');
ok( $s->(5, 0, 1), 'multiple with delivery_tag 0 settles all outstanding');
