# Connection-lifetime state is reset by a successful close, so the same object
# can be reconnected. Each case fails if its reset is missing.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 7;

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
topology($a, 1, $x, $q, $k);
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

# Channels are all closed and forgotten, so a reconnect reopens them. Without
# closing from the end of the list, channel 2 stays in channels[]; the
# confirm_select below then skips amqp_channel_open and issues confirm.select
# on a channel that was never opened on this connection.
my $c = conn();
my ($x3, $q3, $k3) = (uname('x3'), uname('q3'), 'k.life3');
topology($c, 1, $x3, $q3, $k3);
$c->send(channel => 2, exchange => $x3, routing_key => $k3, body => 'open channel 2');
$c->close;
$c->connect;
topology($c, 1, $x3, $q3, $k3);   # the auto-delete topology went with the old connection
ok(eval { $c->confirm_select(channel => 2); 1 }, 'channel 2 is reopened after a reconnect')
    or diag($@);
is($c->send(channel => 2, exchange => $x3, routing_key => $k3, body => 'reopened',
            mandatory => 1, settle_timeout => 5), 'ack', 'confirmed publish on the reopened channel 2');
$c->close;
