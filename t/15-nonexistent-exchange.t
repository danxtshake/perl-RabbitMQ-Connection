# Publishing to an exchange that does not exist makes the broker close the
# channel (404 NOT_FOUND) instead of settling: the wait croaks; unknown outcome.
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker conn uname);
skip_unless_broker();
plan tests => 2;

my $c = conn();
$c->confirm_select(channel => 1);
my $err = '';
eval { $c->send(channel => 1, exchange => uname('no-such-exchange'), routing_key => 'k',
                body => 'x', mandatory => 1, settle_timeout => 5); 1 } or $err = $@;
like($err, qr/channel error 404|NOT_FOUND/, 'non-existent exchange -> channel.close 404 -> croak');
like($err, qr/settlement wait/, 'the error names the settlement wait');
