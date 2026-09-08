# nack: a queue that rejects publishes (max-length + overflow=reject-publish,
# applied by policy) makes the broker nack the confirmed publish. Isolated
# exchange with exactly one bound queue, so "nack" is unambiguous here.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn ctl skip_unless_ctl uname);
skip_unless_broker();
skip_unless_ctl();
plan tests => 4;

my ($x, $q, $k) = (uname('x'), uname('nackq'), 'k.nack');
ok(ctl('set_policy', uname('nackpol'), "'^\Q$q\E\$'", q('{"max-length":1,"overflow":"reject-publish"}'), '--apply-to', 'queues'),
   'policy: max-length 1, overflow reject-publish');

my $c = conn();
$c->declare_exchange(channel => 1, exchange => $x, type => 'topic', durable => 0, auto_delete => 1);
$c->declare_queue(channel => 1, queue => $q, durable => 0, exclusive => 0, auto_delete => 0);
$c->bind(channel => 1, queue => $q, exchange => $x, routing_key => $k);
$c->confirm_select(channel => 1);

is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'fills the queue',
            mandatory => 1, settle_timeout => 5), 'ack', 'first publish fits -> ack');
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'rejected',
            mandatory => 1, settle_timeout => 5), 'nack', 'second publish rejected by the queue -> nack');
$c->close;

ok(ctl('clear_policy', uname('nackpol')) && ctl('delete_queue', $q), 'cleanup policy and queue');
