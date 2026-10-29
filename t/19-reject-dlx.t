# Queue arguments and basic.reject: a queue declared with a dead-letter
# exchange dead-letters a message rejected with requeue => 0, and redelivers
# one rejected with requeue => 1. A queue declared without arguments has none.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 23;

my $c = conn();
my ($x, $dlx) = (uname('x'), uname('dlx'));
my ($q, $dlq) = (uname('work'), uname('dead'));
my ($k, $dk) = ('k.work', 'k.dead');

$c->declare_exchange(channel => 1, exchange => $x, type => 'topic', durable => 0, auto_delete => 1);
$c->declare_exchange(channel => 1, exchange => $dlx, type => 'topic', durable => 0, auto_delete => 1);
my $name = $c->declare_queue(channel => 1, queue => $q, durable => 0, exclusive => 1, auto_delete => 1,
                             arguments => { 'x-dead-letter-exchange'    => $dlx,
                                            'x-dead-letter-routing-key' => $dk });
is($name, $q, 'declare_queue with arguments returns the queue name');
$c->declare_queue(channel => 1, queue => $dlq, durable => 0, exclusive => 1, auto_delete => 1);
$c->bind(channel => 1, queue => $q, exchange => $x, routing_key => $k);
$c->bind(channel => 1, queue => $dlq, exchange => $dlx, routing_key => $dk);

# Redeclaring with the same arguments is equivalent, so it is accepted.
ok(eval { $c->declare_queue(channel => 1, queue => $q, durable => 0, exclusive => 1, auto_delete => 1,
                            arguments => { 'x-dead-letter-exchange'    => $dlx,
                                           'x-dead-letter-routing-key' => $dk }); 1 },
   'redeclare with identical arguments accepted') or diag($@);

$c->consume(channel => 1, queue => $q,   consumer_tag => 'work', no_ack => 0);
$c->consume(channel => 1, queue => $dlq, consumer_tag => 'dead', no_ack => 0);

# requeue => 0: dead-lettered through the queue's DLX.
$c->send(channel => 1, exchange => $x, routing_key => $k, body => 'to the dlq', message_id => 'm1');
my $m = $c->receive(timeout => 5);
ok($m, 'work delivery received') or BAIL_OUT('no delivery');
is($m->{consumer_tag}, 'work', 'delivered on the work queue');
ok(!$m->{redelivered}, 'first delivery is not redelivered');
$c->reject(channel => 1, delivery_tag => $m->{delivery_tag}, requeue => 0);

$m = $c->receive(timeout => 5);
ok($m, 'dead-lettered delivery received') or BAIL_OUT('no dead-letter delivery');
is($m->{consumer_tag}, 'dead', 'rejected (requeue 0) message arrives on the dead-letter queue');
is($m->{body}, 'to the dlq', 'dead-lettered body intact');
is($m->{exchange}, $dlx, 'arrived through the dead-letter exchange');
is($m->{routing_key}, $dk, 'with the dead-letter routing key');
is($m->{props}{message_id}, 'm1', 'message_id survives dead-lettering');
$c->send_ack(channel => 1, delivery_tag => $m->{delivery_tag});

# requeue => 1: redelivered on the same queue, not dead-lettered.
$c->send(channel => 1, exchange => $x, routing_key => $k, body => 'try again');
$m = $c->receive(timeout => 5);
is($m->{consumer_tag}, 'work', 'second message on the work queue');
$c->reject(channel => 1, delivery_tag => $m->{delivery_tag}, requeue => 1);
$m = $c->receive(timeout => 5);
ok($m, 'requeued delivery received') or BAIL_OUT('no redelivery');
is($m->{consumer_tag}, 'work', 'rejected (requeue 1) message comes back on the same queue');
is($m->{body}, 'try again', 'redelivered body intact');
ok($m->{redelivered}, 'marked redelivered');
$c->send_ack(channel => 1, delivery_tag => $m->{delivery_tag});
ok(!defined $c->receive(timeout => 1), 'nothing reached the dead-letter queue on requeue 1');
$c->close;

# A queue declared without arguments has none: redeclaring it with a DLX is an
# inequivalent declaration, which the broker refuses (406). Separate connection,
# since the broker closes the channel.
my $d = conn();
my $plain = uname('plain');
$d->declare_queue(channel => 1, queue => $plain, durable => 0, exclusive => 1, auto_delete => 1);
eval { $d->declare_queue(channel => 1, queue => $plain, durable => 0, exclusive => 1, auto_delete => 1,
                         arguments => { 'x-dead-letter-exchange' => $dlx }); 1 } or my $e = $@;
like($e // '', qr/406/, 'declare_queue without arguments sets none (adding one is inequivalent)');

# A non-hash arguments value is refused before anything is sent.
my $f = conn();
eval { $f->declare_queue(channel => 1, queue => uname('bad'), exclusive => 1, arguments => [1]); 1 } or $e = $@;
like($e // '', qr/arguments must be a hash reference/, 'non-hash arguments croak');
undef $e;
eval { $f->declare_queue(channel => 1, queue => uname('badval'), exclusive => 1,
                         arguments => { 'x-dead-letter-exchange' => [] }); 1 } or $e = $@;
like($e // '', qr/must be a string or an integer/, 'a reference as an argument value croaks');
# An integer value is sent as an integer: the broker refuses x-max-length as a
# string (406), so a successful declare proves the type.
ok(eval { $f->declare_queue(channel => 1, queue => uname('int'), exclusive => 1, auto_delete => 1,
                            arguments => { 'x-max-length' => 10 }); 1 },
   'integer argument value accepted by the broker') or diag($@);
$f->close;

# ...and the same value as a Perl string goes as a longstr, which it refuses.
# The broker closes the channel, so this connection is discarded, not closed.
my $h = conn();
undef $e;
eval { $h->declare_queue(channel => 1, queue => uname('str'), exclusive => 1, auto_delete => 1,
                         arguments => { 'x-max-length' => '10' }); 1 } or $e = $@;
like($e // '', qr/406/, 'string argument value is sent as a string');
undef $h;

# reject is a consumer operation: publisher-confirm connections are publish-only.
my $g = conn();
$g->confirm_select(channel => 1);
undef $e;
eval { $g->reject(channel => 1, delivery_tag => 1, requeue => 0); 1 } or $e = $@;
like($e // '', qr/reject refused: .*publish-only/, 'reject refused on a publisher-confirm connection');
$g->close;
