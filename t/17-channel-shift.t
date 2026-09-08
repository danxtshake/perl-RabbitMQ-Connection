# remove_channel shifts channels, confirm_mode and next_tag together. Closing a
# channel from the application reaches it without poisoning the connection, so
# the surviving channel stays observable.
#
# The two channels are deliberately asymmetric, or the shift is invisible:
# channel 1 is opened but never confirmed, and channel 2 publishes several times
# first. Dropping confirm_mode in the shift would leave channel 2 looking like a
# plain channel (send returns nothing instead of a settlement); dropping
# next_tag would leave the client waiting for a sequence number the broker has
# already passed, timing out instead of acking.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname topology);
skip_unless_broker();
plan tests => 5;

my $c = conn();
my ($x, $q, $k) = (uname('x'), uname('q'), 'k.shift');
topology($c, 1, $x, $q, $k);        # opens channel 1, which is never confirmed
$c->confirm_select(channel => 2);

for my $i (1 .. 3) {
    is($c->send(channel => 2, exchange => $x, routing_key => $k, body => "ch2 seq $i",
                mandatory => 1, settle_timeout => 5), 'ack', "channel 2 publish $i -> ack");
}

# Removes index 0; channel 2 and its confirm state shift down into it.
$c->close_channel(1);

is($c->send(channel => 2, exchange => $x, routing_key => $k, body => 'ch2 seq 4',
            mandatory => 1, settle_timeout => 5), 'ack', 'channel 2 keeps confirm mode and sequence after the shift');
is($c->send(channel => 2, exchange => $x, routing_key => $k, body => 'ch2 seq 5',
            mandatory => 1, settle_timeout => 5), 'ack', 'channel 2 stays in step on the next publish');
$c->close;
