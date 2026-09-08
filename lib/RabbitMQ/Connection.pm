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

RabbitMQ::Connection - thin XS client over librabbitmq-c

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
C<confirm_select> croaks if the connection has consumed, C<consume> croaks
once a channel is in confirm mode, and C<send> croaks on a channel that is not
in confirm mode.

A C<timeout>, or a croak from a broker close or protocol failure, leaves the
connection unusable: the outcome is unknown and settlement frames may still be
in flight. A further confirmed C<send> is refused; discard the connection.

C<send> on a connection that uses no publisher confirms returns nothing, as
before.

=head2 last_return

Returns metadata for the most recent returned mandatory publication as a hash
reference with C<reply_code>, C<reply_text>, C<exchange>, C<routing_key> and
C<body_len>, or C<undef>. The body is drained and not retained.

=cut
