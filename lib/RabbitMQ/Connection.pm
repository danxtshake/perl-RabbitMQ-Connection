package RabbitMQ::Connection;

use strict;
use warnings;

our $VERSION = '0.9';

require XSLoader;
XSLoader::load('RabbitMQ::Connection', $VERSION);

sub new {
    my ($class, %args) = @_;
    $class->_new(\%args);
}

sub declare_queue {
    my ($self, %args) = @_;
    $self->_declare_queue(\%args);
}

sub declare_exchange {
    my ($self, %args) = @_;
    $self->_declare_exchange(\%args);
}

sub send {
    my ($self, %args) = @_;
    $self->_send(\%args);
}

sub confirm_select {
    my ($self, %args) = @_;
    $self->_confirm_select(\%args);
}

sub receive {
    my ($self, %args) = @_;
    $self->_receive(\%args);
}

sub send_ack {
    my ($self, %args) = @_;
    $self->_send_ack(\%args);
}

sub bind {
    my ($self, %args) = @_;
    $self->_bind(\%args);
}

sub consume {
    my ($self, %args) = @_;
    $self->_consume(\%args);
}

1;

__END__

=head1 NAME

RabbitMQ::Connection - thin XS client over librabbitmq-c, with publisher confirms

=head1 SYNOPSIS

    my $c = RabbitMQ::Connection->new(host => 'broker', max_channels => 1);
    $c->connect;

    # Fire-and-forget (unchanged since 0.8): returns "sent" once the frames
    # were written to the socket. That is NOT broker acceptance.
    $c->send(channel => 1, exchange => 'x', routing_key => 'k', body => $msg);

    # Publisher confirms (0.9): put the channel into confirm mode, then every
    # send on it waits for the broker to settle the publication.
    $c->confirm_select(channel => 1);
    my $status = $c->send(channel => 1, exchange => 'x', routing_key => 'k',
                          body => $msg, mandatory => 1, settle_timeout => 10);
    if ($status eq 'ack') {
        # the broker owns the message and routed it to at least one queue
    }
    elsif ($status eq 'returned') {
        my $r = $c->last_return;   # { reply_code, reply_text, exchange, routing_key, body_len }
    }
    elsif ($status eq 'nack' or $status eq 'timeout') {
        # outcome unknown or refused: discard $c, reconnect, retry later
    }

=head1 PUBLISHER CONFIRMS

=head2 confirm_select(channel => N)

Issues C<confirm.select> on the channel (opening it if needed). From then on
C<send> on that channel blocks until the publication is settled. Confirm
sequence numbers are kept per channel, starting at 1 after C<confirm_select>.

Confirm mode is only allowed on a B<publish-only connection>: C<confirm_select>
croaks if the connection has consumed, and C<consume> croaks once any channel
is in confirm mode. This keeps settlement frames from ever interleaving with
deliveries, which is what lets the settlement loop stay one-message-in-flight.

=head2 send(..., mandatory => 1, settle_timeout => SECONDS)

On a confirm-mode channel, returns one of:

=over 4

=item C<ack>

C<basic.ack> arrived for this publication and no C<basic.return> preceded it:
the broker owns the message and routed it to at least one bound queue.
B<Not> proof that a particular consumer's queue exists — any queue bound to
the key satisfies it.

=item C<returned>

The publication was C<mandatory> and routed to no queue: the broker sent
C<basic.return> (with the whole message, which is drained and discarded) and
then its C<basic.ack>. A C<basic.ack> on its own is therefore B<not> success
when C<mandatory> is used; this method already folds the two together.
Details are in C<last_return>.

=item C<nack>

C<basic.nack>: the broker could not take responsibility (an internal error, or
a queue that rejects the publish). With several queues bound to the key the
message may still have reached some of them.

=item C<timeout>

Nothing settled the publication within C<settle_timeout> seconds (default 10).
The outcome is B<unknown> — the broker may or may not own the message — and
the connection's settlement state is no longer known either: B<discard this
connection> before retrying.

=back

A C<channel.close> or C<connection.close> from the broker, a socket error or an
unexpected frame croaks. Treat a croak like C<timeout>: unknown, discard the
connection. C<connection.blocked> / C<connection.unblocked> (resource alarms)
are informational: the wait continues and C<is_blocked> reports the state.

On a channel that is not in confirm mode, C<send> returns C<"sent">.

=head2 last_return

The C<basic.return> of the most recent confirm-mode publication on this
connection, as a hash reference with C<reply_code>, C<reply_text>,
C<exchange>, C<routing_key> and C<body_len>, or C<undef>. The body is never
retained.

=head2 is_blocked

True while the broker has this connection blocked by a resource alarm.

=head1 DELIVERY SEMANTICS

Messages are always published persistent (C<delivery_mode> 2). With confirms,
C<ack> means the broker has taken responsibility; nothing here makes delivery
exactly-once — a caller that persists its own outgoing record and deletes it
only after C<ack> gets at-least-once delivery, with a duplicate possible if it
dies between the C<ack> and its own delete.

=cut
