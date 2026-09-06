# returned: a mandatory publish that routes nowhere is returned and THEN acked;
# the API reports "returned", never "ack"; the returned body (even a large,
# multi-frame one) is fully drained so the next publish settles normally.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 12;

my $c = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.bound');
topology($c, 1, $x, $q, $k);
$c->confirm_select(channel => 1);

my $st = $c->send(channel => 1, exchange => $x, routing_key => 'k.unbound', body => 'nobody home',
                  mandatory => 1, settle_timeout => 5);
is($st, 'returned', 'mandatory publish to an unbound key -> returned');
my $r = $c->last_return;
ok(ref $r eq 'HASH', 'last_return is a hash');
is($r->{reply_code}, 312, 'reply code 312 NO_ROUTE');
is($r->{routing_key}, 'k.unbound', 'routing key reported');
is($r->{exchange}, $x, 'exchange reported');
is($r->{body_len}, length('nobody home'), 'body length reported, body not retained');

is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'after return',
            mandatory => 1, settle_timeout => 5), 'ack', 'stream in sync: next routable publish -> ack');
ok(!defined $c->last_return, 'last_return cleared by the next publish');

# Large body: many frames come back with the return and must all be drained.
my $big = 'x' x 1_000_000;
is($c->send(channel => 1, exchange => $x, routing_key => 'k.unbound', body => $big,
            mandatory => 1, settle_timeout => 10), 'returned', '1 MB mandatory publish to an unbound key -> returned');
is($c->last_return->{body_len}, length $big, 'large returned body fully drained (length matches)');
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'still in sync',
            mandatory => 1, settle_timeout => 5), 'ack', 'stream still in sync after a large return');

# Without mandatory the broker silently drops an unroutable message and acks it.
is($c->send(channel => 1, exchange => $x, routing_key => 'k.unbound', body => 'dropped',
            mandatory => 0, settle_timeout => 5), 'ack', 'non-mandatory unroutable publish -> ack (silently dropped by the broker)');
$c->close;
