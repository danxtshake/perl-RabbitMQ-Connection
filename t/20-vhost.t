# vhost: a connection logs in to the vhost it is given, not always "/".
# (hactl#46: the argument was looked up under a key 8 bytes long, so it was
# never found and every connection used the default.)
use strict; use warnings;
use lib 't/lib';
use Test::More;
use RmqcTest qw(skip_unless_broker skip_unless_ctl conn ctl uname);
skip_unless_broker();
skip_unless_ctl();
plan tests => 4;

my $vhost = uname('vh');
ok(ctl('add_vhost', $vhost) && ctl('set_permissions', '-p', $vhost, $ENV{TEST_USER} || 'guest', "'.*'", "'.*'", "'.*'"),
   "vhost $vhost created for the test user");

my $c = conn(vhost => $vhost);
my $q = uname('vq');
$c->declare_queue(channel => 1, queue => $q, durable => 0, exclusive => 0, auto_delete => 0);
my $there = `$ENV{TEST_CTL} list_queues -q -p $vhost name 2>/dev/null`;
my $root  = `$ENV{TEST_CTL} list_queues -q -p / name 2>/dev/null`;
like($there, qr/^\Q$q\E$/m, 'a queue declared on the connection is in the vhost it was given');
unlike($root, qr/^\Q$q\E$/m, 'and not in "/"');
$c->close;

ok(!eval { conn(vhost => uname('nosuch')); 1 }, 'a vhost that does not exist is refused, not replaced by "/"');

ctl('delete_vhost', $vhost);
