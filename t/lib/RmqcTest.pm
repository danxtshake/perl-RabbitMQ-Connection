package RmqcTest;
# Shared helpers for the live-broker tests. Every test skips unless TEST_HOST is
# set; tests that drive the broker (policies, alarms, forced closes) also need
# TEST_CTL, a command prefix that runs rabbitmqctl against that broker, e.g.
#   TEST_CTL="docker exec rmqc-test-broker rabbitmqctl"
# Set TEST_REQUIRE_BROKER to fail instead of skipping when either is missing.
use strict;
use warnings;
use Test::More;
use RabbitMQ::Connection;

our @EXPORT_OK = qw(skip_unless_broker skip_unless_ctl conn ctl uname topology);
use Exporter 'import';

sub _unavailable {
    my ($why) = @_;
    BAIL_OUT($why) if $ENV{TEST_REQUIRE_BROKER};
    plan skip_all => $why;
}

sub skip_unless_broker {
    _unavailable('set TEST_HOST (and TEST_PORT/TEST_USER/TEST_PASS/TEST_VHOST) to a test broker')
        unless $ENV{TEST_HOST};
}

sub skip_unless_ctl {
    _unavailable('set TEST_CTL to a rabbitmqctl command prefix')
        unless defined $ENV{TEST_CTL} && length $ENV{TEST_CTL};
}

# Run rabbitmqctl with the configured prefix; returns true on success.
sub ctl {
    my @args = @_;
    return system("$ENV{TEST_CTL} @args >/dev/null 2>&1") == 0;
}

sub conn {
    my (%over) = @_;
    my $c = RabbitMQ::Connection->new(
        host         => $ENV{TEST_HOST},
        port         => $ENV{TEST_PORT} || 5672,
        user         => $ENV{TEST_USER} || 'guest',
        pass         => $ENV{TEST_PASS} || 'guest',
        vhost        => $ENV{TEST_VHOST} || '/',
        tls          => 0,
        heartbeat    => 5,
        max_channels => 2,
        %over,
    );
    $c->connect;
    return $c;
}

# Per-process unique names so parallel or repeated runs never collide.
sub uname { my $s = shift; "rmqc-test-$s-$$" }

# Declare an auto-deleting topic exchange and an exclusive queue bound to $key.
sub topology {
    my ($c, $channel, $exchange, $queue, $key) = @_;
    $c->declare_exchange(channel => $channel, exchange => $exchange, type => 'topic',
                         durable => 0, auto_delete => 1);
    $c->declare_queue(channel => $channel, queue => $queue, durable => 0,
                      exclusive => 1, auto_delete => 1);
    $c->bind(channel => $channel, queue => $queue, exchange => $exchange, routing_key => $key);
}

1;
