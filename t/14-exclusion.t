# Confirm mode is for publish-only connections, enforced both ways.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 3;

# consume first, then confirm_select -> refused
my $a = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.excl');
topology($a, 1, $x, $q, $k);
$a->consume(channel => 1, queue => $q, no_ack => 1);
eval { $a->confirm_select(channel => 2); 1 } or my $e1 = $@;
like($e1, qr/publish-only/, 'confirm_select after consume is refused');
$a->close;

# confirm_select first, then consume -> refused, on the same and on another channel
my $b = conn();
topology($b, 1, $x, uname('q2'), $k);
$b->confirm_select(channel => 1);
eval { $b->consume(channel => 1, queue => uname('q2'), no_ack => 1); 1 } or my $e2 = $@;
like($e2, qr/confirm mode/, 'consume after confirm_select is refused (same channel)');
eval { $b->consume(channel => 2, queue => uname('q2'), no_ack => 1); 1 } or my $e3 = $@;
like($e3, qr/confirm mode/, 'consume after confirm_select is refused (other channel: connection-wide)');
$b->close;
