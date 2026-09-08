# A successful close resets connection-lifetime state, so the same object can be
# reconnected. Each case fails if its reset is missing.
#
# Reconnecting a connection that had more than one channel open additionally
# needs the rmqc_close() iteration fix (upstream PR), so that case is not here.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 5;

# has_consumer: without the reset, confirm_select on the reconnected object is
# refused because the previous connection consumed.
my $a = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.life');
topology($a, 1, $x, $q, $k);
$a->consume(channel => 1, queue => $q, no_ack => 1);
$a->close;
$a->connect;
ok(eval { $a->confirm_select(channel => 1); 1 }, 'confirm_select allowed after a clean close of a consuming connection')
    or diag($@);
topology($a, 1, $x, $q, $k);   # the auto-delete topology went with the old connection
is($a->send(channel => 1, exchange => $x, routing_key => $k, body => 'after reconnect',
            mandatory => 1, settle_timeout => 5), 'ack', 'confirmed publish works on the reconnected object');
$a->close;

# last_return: without the reset, the reconnected object still reports the
# previous connection's return.
my $b = conn();
my ($x2, $q2, $k2) = (uname('x2'), uname('q2'), 'k.life2');
topology($b, 1, $x2, $q2, $k2);
$b->confirm_select(channel => 1);
is($b->send(channel => 1, exchange => $x2, routing_key => 'k.unbound', body => 'nobody home',
            mandatory => 1, settle_timeout => 5), 'returned', 'mandatory unroutable publish -> returned');
ok(defined $b->last_return, 'last_return populated before close');
$b->close;
$b->connect;
ok(!defined $b->last_return, 'last_return cleared by a successful close');
$b->close;
