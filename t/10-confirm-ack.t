# ack: a routable confirmed publish settles as "ack"; sequence numbers are per
# channel; a non-confirm channel still returns "sent".
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 12;

my $c = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.ack');
topology($c, 1, $x, $q, $k);

is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'plain'), 'sent',
   'send on a non-confirm channel returns "sent"');

$c->confirm_select(channel => 1);
for my $i (1 .. 3) {
    is($c->send(channel => 1, exchange => $x, routing_key => $k, body => "m$i",
                mandatory => 1, settle_timeout => 5), 'ack', "confirmed publish $i -> ack");
}
ok(!defined $c->last_return, 'no basic.return recorded after acks');

# Idempotent confirm_select: a second call must not re-send confirm.select or
# reset the client sequence, or the next ack's tag would never match.
$c->confirm_select(channel => 1);
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'after 2nd confirm_select',
            mandatory => 1, settle_timeout => 5), 'ack', 'publish after a repeated confirm_select -> ack');

# Connection profile: once a channel confirms, fire-and-forget on another channel is refused.
eval { $c->send(channel => 2, exchange => $x, routing_key => $k, body => 'unconfirmed'); 1 } or my $e = $@;
like($e, qr/publisher-confirm mode/, 'send on a non-confirm channel is refused once the connection confirms');
ok(!defined $c->last_return, 'refusal recorded nothing');
ok(!$c->is_blocked, 'connection not blocked');

# Per-channel sequence numbers: a second confirm channel starts its own count.
# With a connection-global counter the expected tag would never match and the
# call would time out instead of acking.
$c->confirm_select(channel => 2);
is($c->send(channel => 2, exchange => $x, routing_key => $k, body => 'ch2',
            mandatory => 1, settle_timeout => 5), 'ack', 'first publish on channel 2 -> ack');
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'ch1 again',
            mandatory => 1, settle_timeout => 5), 'ack', 'channel 1 keeps its own sequence');
is($c->send(channel => 2, exchange => $x, routing_key => $k, body => 'ch2 again',
            mandatory => 1, settle_timeout => 5), 'ack', 'channel 2 keeps its own sequence');
$c->close;
