# ack: a routable confirmed publish settles as "ack", and sequence numbers are
# tracked per channel.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 12;

my $c = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.ack');
topology($c, 1, $x, $q, $k);

# Compatibility: an unconfirmed send must still yield no value, in either
# context. Only a confirmed send reports a settlement result.
my $plain = $c->send(channel => 1, exchange => $x, routing_key => $k, body => 'plain');
ok(!defined $plain, 'unconfirmed send returns undef in scalar context, as before');
my @plain = $c->send(channel => 1, exchange => $x, routing_key => $k, body => 'plain');
is(scalar @plain, 0, 'unconfirmed send returns an empty list, as before');

$c->confirm_select(channel => 1);
for my $i (1 .. 3) {
    is($c->send(channel => 1, exchange => $x, routing_key => $k, body => "m$i",
                mandatory => 1, settle_timeout => 5), 'ack', "confirmed publish $i -> ack");
}
ok(!defined $c->last_return, 'no basic.return recorded after acks');

# A second confirm_select must not reset the sequence, or the next ack's tag
# would never match.
$c->confirm_select(channel => 1);
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'after 2nd confirm_select',
            mandatory => 1, settle_timeout => 5), 'ack', 'publish after a repeated confirm_select -> ack');

# Mixed confirmed/unconfirmed publishing on one connection is refused.
eval { $c->send(channel => 2, exchange => $x, routing_key => $k, body => 'unconfirmed'); 1 } or my $e = $@;
like($e, qr/publisher confirms/, 'send on a non-confirm channel is refused once the connection confirms');
ok(!defined $c->last_return, 'refusal recorded nothing');

# Per-channel sequence numbers: with a connection-global counter the expected
# tag would never match and these would time out instead of acking.
$c->confirm_select(channel => 2);
is($c->send(channel => 2, exchange => $x, routing_key => $k, body => 'ch2',
            mandatory => 1, settle_timeout => 5), 'ack', 'first publish on channel 2 -> ack');
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'ch1 again',
            mandatory => 1, settle_timeout => 5), 'ack', 'channel 1 keeps its own sequence');
is($c->send(channel => 2, exchange => $x, routing_key => $k, body => 'ch2 again',
            mandatory => 1, settle_timeout => 5), 'ack', 'channel 2 keeps its own sequence');
$c->close;
