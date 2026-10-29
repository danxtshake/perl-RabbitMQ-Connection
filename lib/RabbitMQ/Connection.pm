package RabbitMQ::Connection;

use strict;
use warnings;

our $VERSION = '0.91';

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

sub reject {
    my ($self, %args) = @_;
    $self->_reject(\%args);
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

RabbitMQ::Connection - thin XS client over librabbitmq-c

=head1 METHODS

Arguments are key/value pairs. This section covers the methods and arguments
added in 0.91; the other methods (C<connect>, C<declare_exchange>, C<bind>,
C<consume>, C<send_ack>, C<close>) are not documented here yet.

=head2 declare_queue(queue => $name, channel => N, durable => 0|1, exclusive => 0|1, auto_delete => 0|1, passive => 0|1, arguments => \%args)

C<arguments> is optional: a hash reference of queue arguments sent as the
C<queue.declare> argument table, for example

    $c->declare_queue(queue => $q, durable => 1, arguments => {
        'x-dead-letter-exchange'    => 'activities',
        'x-dead-letter-routing-key' => 'activity.dead',
    });

A string value is sent as a long string. A value that is a Perl integer, and
not a string, is sent as a signed 64-bit integer, which arguments such as
C<x-message-ttl> and C<x-max-length> need: write C<10>, not C<'10'>. Any other
value (undef, a reference, a non-integer number) croaks, as does an
C<arguments> value that is not a hash reference. Without C<arguments>, or with
C<undef>, the queue is declared with no arguments, as before.

RabbitMQ refuses to redeclare an existing queue with different arguments
(channel error 406), which croaks and closes the channel.

=head2 send(exchange => $x, routing_key => $k, body => $b, channel => N, mandatory => 0|1, content_type => $type, message_id => $id)

C<content_type> sets the C<content-type> property; it defaults to
C<text/plain>. C<message_id> sets the C<message-id> property; without it the
message has none. Either given as C<undef> counts as not given. Messages are
always published persistent (C<delivery-mode> 2). See L</PUBLISHER CONFIRMS>
for C<settle_timeout> and the return value.

=head2 receive(timeout => $seconds)

Returns the next delivery as a hash reference with C<channel>,
C<delivery_tag>, C<redelivered>, C<exchange>, C<consumer_tag>, C<routing_key>
and C<body>, or C<undef> on timeout. It also has C<props>, a hash reference
holding those of C<content_type>, C<message_id> and C<delivery_mode> that the
publisher set (an empty hash when it set none):

    my $m = $c->receive(timeout => 5);
    my $id = $m->{props}{message_id};

=head2 reject(delivery_tag => N, channel => N, requeue => 0|1)

Rejects one delivery with C<basic.reject>. C<delivery_tag> is required;
C<channel> defaults to 1, and must be the channel the delivery arrived on.
C<requeue> defaults to 1, the protocol default.

With C<requeue =E<gt> 0> the broker discards the message, which dead-letters
it through the queue's C<x-dead-letter-exchange> (with its
C<x-dead-letter-routing-key>, if set) when the queue has one. With
C<requeue =E<gt> 1> the message goes back on its queue and is redelivered with
C<redelivered> set.

Only a delivery consumed with C<no_ack =E<gt> 0> can be rejected; rejecting an
unknown or already settled delivery tag is a channel error that the broker
reports asynchronously, on the connection's next read. C<reject> is a consumer
operation, so it croaks on a publisher-confirm connection (see below).

=head1 PUBLISHER CONFIRMS

=head2 confirm_select(channel => N)

Enables publisher confirms for a channel, opening it if needed. Calling it
again on a channel already in confirm mode does nothing.

After confirmation is enabled, C<send> on that channel waits for broker
settlement and returns:

=over 4

=item C<ack>

RabbitMQ positively settled the publication. With C<mandatory =E<gt> 1>, no
C<basic.return> preceded the ack.

=item C<returned>

A C<mandatory> publication was unroutable. See C<last_return>.

=item C<nack>

The broker negatively acknowledged the publication.

=item C<timeout>

The settlement outcome is unknown. Nothing settled within C<settle_timeout>
seconds (default 10).

=back

    $c->confirm_select(channel => 1);
    my $status = $c->send(channel => 1, exchange => $x, routing_key => $k,
                          body => $msg, mandatory => 1, settle_timeout => 10);

Three things are distinct. C<send> returning at all means only that the frames
were written locally. C<ack> means RabbitMQ settled the publication. Neither
proves that a particular consumer's queue exists: any queue bound to the key
satisfies the ack. Routing failure is reported as C<returned>, and only when
C<mandatory> is set.

Confirm sequence numbers are per channel, but settlement is read synchronously
for the whole connection, so one confirmed publication settles at a time.

Publisher-confirm connections must be publish-only, because this
implementation reads settlement frames synchronously from the connection:
C<confirm_select> croaks if the connection has consumed, C<consume> and
C<reject> croak once a channel is in confirm mode, and C<send> croaks on a
channel that is not in confirm mode.

A C<timeout>, or a croak from a broker close or protocol failure, leaves the
connection unusable: the outcome is unknown and settlement frames may still be
in flight. A further confirmed C<send> is refused. Discard the object; calling
C<close> and C<connect> on it does not clear that state.

A connection closed cleanly may be reconnected and reused.

C<send> on a connection that uses no publisher confirms returns nothing, as
before.

=head2 last_return

Returns metadata for the most recent returned mandatory publication as a hash
reference with C<reply_code>, C<reply_text>, C<exchange>, C<routing_key> and
C<body_len>, or C<undef>. The body is drained and not retained.

=cut
