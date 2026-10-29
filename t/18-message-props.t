# Message properties: send sets content_type and message_id when given, and
# receive reports them under props. Without them, send behaves as before:
# text/plain, persistent, no message_id.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 15;

my $c = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.props');
topology($c, 1, $x, $q, $k);
$c->consume(channel => 1, queue => $q, no_ack => 1);

my $plain = $c->send(channel => 1, exchange => $x, routing_key => $k, body => 'plain');
ok(!defined $plain, 'unconfirmed send with no new args still returns nothing');
my $m = $c->receive(timeout => 5);
ok($m, 'received the plain message') or BAIL_OUT('no delivery');
is($m->{body}, 'plain', 'body unchanged');
is($m->{props}{content_type}, 'text/plain', 'default content_type is still text/plain');
ok(!exists $m->{props}{message_id}, 'no message_id unless one is given');
is($m->{props}{delivery_mode}, 2, 'delivery_mode is still 2 (persistent)');

$c->send(channel => 1, exchange => $x, routing_key => $k, body => '{"a":1}',
         content_type => 'application/json', message_id => 'act-0001');
$m = $c->receive(timeout => 5);
ok($m, 'received the message with properties') or BAIL_OUT('no delivery');
is($m->{props}{content_type}, 'application/json', 'content_type delivered');
is($m->{props}{message_id}, 'act-0001', 'message_id delivered');
is($m->{props}{delivery_mode}, 2, 'delivery_mode stays 2 when properties are given');

# Existing keys of the receive hash are unchanged.
is_deeply([sort grep { $_ ne 'props' } keys %$m],
          [sort qw(channel delivery_tag redelivered exchange consumer_tag routing_key body)],
          'receive keeps its existing keys');
is($m->{exchange}, $x, 'exchange key unchanged');
is($m->{routing_key}, $k, 'routing_key key unchanged');

# message_id alone keeps the default content_type.
$c->send(channel => 1, exchange => $x, routing_key => $k, body => 'id only', message_id => 'act-0002');
$m = $c->receive(timeout => 5);
is($m->{props}{message_id}, 'act-0002', 'message_id alone delivered');
is($m->{props}{content_type}, 'text/plain', 'content_type defaults when only message_id is given');
$c->close;
