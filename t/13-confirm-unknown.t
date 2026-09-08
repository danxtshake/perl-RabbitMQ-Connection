# unknown outcomes: a stalled settlement returns "timeout" and a broker close
# croaks; both must leave the connection refusing further confirmed publishes.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn ctl skip_unless_ctl uname topology);
skip_unless_broker();
skip_unless_ctl();
plan tests => 7;

my ($x, $q, $k) = (uname('x'), uname('q'), 'k.alarm');

# Baseline: routable and fast.
my $c = conn();
topology($c, 1, $x, $q, $k);
$c->confirm_select(channel => 1);
is($c->send(channel => 1, exchange => $x, routing_key => $k, body => 'before alarm',
            mandatory => 1, settle_timeout => 5), 'ack', 'baseline ack');

# Memory alarm: the broker blocks publishing connections; confirms never arrive.
ok(ctl('set_vm_memory_high_watermark', '0'), 'raise a memory alarm (watermark 0)');
sleep 3;   # let the alarm propagate to connections
my $t0 = time;
my $st = eval { $c->send(channel => 1, exchange => $x, routing_key => $k, body => 'stalled',
                         mandatory => 1, settle_timeout => 3) };
my $took = time - $t0;
is($st, 'timeout', 'publish under the alarm -> timeout (outcome unknown)') or diag($@);
ok($took >= 2 && $took <= 6, "bounded wait honoured (${took}s for a 3s settle_timeout)");

# The unsettled publication's frames may still arrive, so the connection must
# not be reused for another confirmed publish.
eval { $c->send(channel => 1, exchange => $x, routing_key => $k, body => 'reuse',
                mandatory => 1, settle_timeout => 3); 1 } or my $reuse = $@;
like($reuse, qr/did not settle/, 'confirmed publish refused after an unknown settlement');

# Broker-initiated close while a settlement is pending -> croak.
my $pid = fork();
if ($pid == 0) { sleep 2; ctl('close_all_connections', "'rmqc test: close during settlement'"); exit 0 }
my $c2 = conn();
$c2->confirm_select(channel => 1);
my $err = '';
eval { $c2->send(channel => 1, exchange => $x, routing_key => $k, body => 'closed under me',
                 mandatory => 1, settle_timeout => 15); 1 } or $err = $@;
waitpid($pid, 0);
like($err, qr/settlement wait|connection error|socket|closed/i, 'broker-initiated close during the wait croaks');

ok(ctl('set_vm_memory_high_watermark', '0.4'), 'restore the watermark');
