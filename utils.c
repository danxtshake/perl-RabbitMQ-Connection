#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "utils.h"

#define DEFAULT_HOST      "localhost"
#define DEFAULT_PORT      5672
#define DEFAULT_TLS       0
#define DEFAULT_USER      "guest"
#define DEFAULT_PASS      "guest"
#define DEFAULT_VHOST     "/"
#define DEFAULT_CHANNEL   1
#define DEFAULT_KEY       "#"
#define DEFAULT_NO_LOCAL  0
#define DEFAULT_EXCLUSIVE 0
#define DEFAULT_NO_ACK    1
#define DEFAULT_EXCH_TYPE "direct"

#define FRAME_MAX    131072
#define HEARTBEAT    0
#define DEFAULT_SETTLE_TIMEOUT 10   /* seconds to wait for publisher settlement */

static int channel_exists(rmqc_t *self, int channel);
static int channel_index(rmqc_t *self, int channel);
static int any_confirm_channel(rmqc_t *self);
static const char *wait_settlement(rmqc_t *self, int channel, uint64_t tag, int timeout_s);
static int settles(uint64_t tag, uint64_t delivery_tag, int multiple);
static void record_return(rmqc_t *self, amqp_basic_return_t *r, size_t body_len);
static void clear_return(rmqc_t *self);
static void store_channel(rmqc_t *self, int channel);
static void remove_channel(rmqc_t *self, int channel);
static int fetch_int(HV *h, char *key, int *val);
static int fetch_uint(HV *h, char *key, unsigned long *val);
static int fetch_str(HV *h, char *key, char **val, int *len);
static void croak_on_amqp_error(amqp_rpc_reply_t x, char const *context);

extern int
rmqc_new(rmqc_t **self, HV *args)
{
    char *host = NULL, *user = NULL, *pass = NULL, *vhost = NULL, *cacert = NULL;
    int len;

    *self = calloc(1, sizeof(**self));
    if(*self == NULL)
        croak("could not initialize instance\n");

    if(!hv_exists(args, "host", strlen("host"))) {
        host = DEFAULT_HOST;
        len = strlen(DEFAULT_HOST);
    }
    else {
        fetch_str(args, "host", &host, &len);
    }

    if(((*self)->host = calloc(len + 1, sizeof(char))) == NULL)
        croak("calloc failed\n");
    strncpy((*self)->host, host, len);
    (*self)->host[len] = '\0';

    if(!hv_exists(args, "port", strlen("port")))
        (*self)->port = DEFAULT_PORT;
    else
        fetch_int(args, "port", &(*self)->port);

    if(!hv_exists(args, "tls", strlen("tls")))
        (*self)->ssl = 0;
    else
        fetch_int(args, "tls", &(*self)->ssl);

    if(!hv_exists(args, "heartbeat", strlen("heartbeat")))
        (*self)->heartbeat = HEARTBEAT;
    else
        fetch_int(args, "heartbeat", &(*self)->heartbeat);

    if(!hv_exists(args, "verify", strlen("verify")))
        (*self)->verify = 0;
    else
        fetch_int(args, "verify", &(*self)->verify);

    if(!hv_exists(args, "cacert", strlen("cacert"))) {
        (*self)->cacert = NULL;
    }
    else {
        fetch_str(args, "cacert", &cacert, &len);
        if(((*self)->cacert = calloc(len + 1, sizeof(char))) == NULL)
            croak("calloc failed\n");
        strncpy((*self)->cacert, cacert, len);
        (*self)->cacert[len] = '\0';
    }

    if(!hv_exists(args, "user", strlen("user"))) {
        user = DEFAULT_USER;
        len = strlen(DEFAULT_USER);
    }
    else {
        fetch_str(args, "user", &user, &len);
    }

    if(((*self)->user = calloc(len + 1, sizeof(char))) == NULL)
        croak("calloc failed\n");
    strncpy((*self)->user, user, len);
    (*self)->user[len] = '\0';

    if(!hv_exists(args, "pass", strlen("pass"))) {
        pass = DEFAULT_PASS;
        len = strlen(DEFAULT_PASS);
    }
    else {
        fetch_str(args, "pass", &pass, &len);
    }

    if(((*self)->pass = calloc(len + 1, sizeof(char))) == NULL)
        croak("calloc failed\n");
    strncpy((*self)->pass, pass, len + 1);
    (*self)->pass[len] = '\0';

    if(!hv_exists(args, "vhost", strlen("vhost\n\n\n"))) {
        vhost = DEFAULT_VHOST;
        len = strlen(DEFAULT_VHOST);
    }
    else {
        fetch_str(args, "vhost", &vhost, &len);
    }

    if(((*self)->vhost = calloc(len + 1, sizeof(char))) == NULL)
        croak("calloc failed\n");
    strncpy((*self)->vhost, vhost, len);
    (*self)->vhost[len] = '\0';

    if(!hv_exists(args, "max_channels", strlen("max_channels")))
        (*self)->max_channels = 1;
    else
        fetch_int(args, "max_channels", &(*self)->max_channels);

    if(((*self)->channels = calloc((*self)->max_channels, sizeof(int))) == NULL)
        croak("could not initialize list of connections\n");
    if(((*self)->confirm_mode = calloc((*self)->max_channels, sizeof(int))) == NULL)
        croak("could not initialize channel state\n");
    if(((*self)->next_tag = calloc((*self)->max_channels, sizeof(uint64_t))) == NULL)
        croak("could not initialize channel state\n");

    return RMQC_OK;
}

/*
 * Establish a connection and login.
 */
extern int
rmqc_connect(rmqc_t *self)
{
    amqp_socket_t *socket = NULL;
    int status;

    self->con = amqp_new_connection();
    if(self->ssl) {
        socket = amqp_ssl_socket_new(self->con);
        if(!socket)
            croak("could not create SSL/TLS socket\n");

        if(self->cacert) {
            status = amqp_ssl_socket_set_cacert(socket, self->cacert);
            if(status)
                croak("could not set CA certificate %s: %s\n",
                      self->cacert, amqp_error_string2(status));
        }

        amqp_ssl_socket_set_verify(socket, self->verify ? 1 : 0);
    }
    else {
        socket = amqp_tcp_socket_new(self->con);
        if(!socket)
            croak("could not create tcp socket\n");
    }

    status = amqp_socket_open(socket, self->host, self->port);
    if(status != 0) {
        croak("could not open %ssocket to %s port %d: %s\n",
              (self->ssl ? "ssl " : ""),
              self->host,
              self->port,
              amqp_error_string2(status));
    }

    croak_on_amqp_error(amqp_login(self->con, self->vhost, self->max_channels,
                                   FRAME_MAX, self->heartbeat, AMQP_SASL_METHOD_PLAIN,
                                   self->user, self->pass), "login");

    return RMQC_OK;
}

/*
 * Open the specified channel if not already open (defaults to 1) and declare
 * an exchange on a connected connection.
 */
extern int
rmqc_declare_exchange(rmqc_t *self, HV *args)
{
    amqp_bytes_t exchange, type;
    char *exchange_name = NULL, *type_name = NULL;
    int channel, passive, durable, auto_delete, internal, len;

    if(self->con == NULL)
        croak("not connected\n");

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    if(fetch_int(args, "passive", &passive) != RMQC_OK)
        passive = 0;
    if(fetch_int(args, "durable", &durable) != RMQC_OK)
        durable = 0;
    if(fetch_int(args, "internal", &internal) != RMQC_OK)
        internal = 0;
    if(fetch_int(args, "auto_delete", &auto_delete) != RMQC_OK)
        auto_delete = 1;

    if(fetch_str(args, "exchange", &exchange_name, &len) != RMQC_OK) {
        exchange = amqp_empty_bytes;
    }
    else {
        exchange.bytes = exchange_name;
        exchange.len = len;
    }

    if(fetch_str(args, "type", &type_name, &len) != RMQC_OK) {
        type = amqp_cstring_bytes(DEFAULT_EXCH_TYPE);
    }
    else {
        type.bytes = type_name;
        type.len = len;
    }

    amqp_exchange_declare(self->con, channel, exchange, type, passive, durable,
                          auto_delete, internal, amqp_empty_table);
    croak_on_amqp_error(amqp_get_rpc_reply(self->con), "declare exchange");

    return RMQC_OK;
}

/*
 * Open the specified channel (defaults to 1) and declare a queue on
 * a connected connection.
 */
extern char
*rmqc_declare_queue(rmqc_t *self, HV *args)
{
    amqp_bytes_t queue;
    char *queue_name = NULL;
    int channel, passive, durable, exclusive, auto_delete, len;

    if(self->con == NULL)
        croak("not connected\n");

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    if(fetch_int(args, "passive", &passive) != RMQC_OK)
        passive = 0;
    if(fetch_int(args, "durable", &durable) != RMQC_OK)
       durable = 0;
    if(fetch_int(args, "exclusive", &exclusive) != RMQC_OK)
        exclusive = 0;
    if(fetch_int(args, "auto_delete", &auto_delete) != RMQC_OK)
        auto_delete = 0;

    if(fetch_str(args, "queue", &queue_name, &len) != RMQC_OK) {
        queue = amqp_empty_bytes;
    }
    else {
        queue.bytes = queue_name;
        queue.len = len;
    }

    amqp_queue_declare(self->con, channel, queue, passive, durable, exclusive,
                       auto_delete, amqp_empty_table);
    croak_on_amqp_error(amqp_get_rpc_reply(self->con), "declare queue");

    return queue.bytes;
}

extern int
rmqc_bind(rmqc_t *self, HV *args)
{
    amqp_bytes_t queue, exchange, key;
    char *queue_name = NULL, *exchange_name = NULL, *key_name = NULL;
    int channel, len;

    if(self->con == NULL)
        croak("not connected\n");

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    if(fetch_str(args, "exchange", &exchange_name, &len) != RMQC_OK) {
        exchange = amqp_empty_bytes;
    }
    else {
        exchange.bytes = exchange_name;
        exchange.len = len;
    }

    if(fetch_str(args, "routing_key", &key_name, &len) != RMQC_OK) {
        key = amqp_cstring_bytes(DEFAULT_KEY);
    }
    else {
        key.bytes = key_name;
        key.len = len;
    }

    if(fetch_str(args, "queue", &queue_name, &len) != RMQC_OK) {
        queue = amqp_empty_bytes;
    }
    else {
        queue.bytes = queue_name;
        queue.len = len;
    }

    amqp_queue_bind(self->con, channel, queue, exchange, key, amqp_empty_table);
    croak_on_amqp_error(amqp_get_rpc_reply(self->con), "failed to bind");

    return RMQC_OK;
}

/*
 * Publish one message. On a channel in publisher-confirm mode this waits for
 * the broker to settle the publication and returns "ack", "returned", "nack"
 * or "timeout"; on any other channel it publishes and returns NULL, so that
 * callers not using confirms see the same (empty) result as before.
 */
extern const char *
rmqc_send(rmqc_t *self, HV *args)
{
    amqp_bytes_t exchange, routing_key, body;
    char *exchange_name = NULL, *key_name = NULL, *body_str = NULL;
    int channel, mandatory, immediate, len, idx, settle_timeout;
    amqp_basic_properties_t props;

    if(self->con == NULL)
        croak("not connected\n");

    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2;

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    if(fetch_int(args, "mandatory", &mandatory) != RMQC_OK)
        mandatory = 0;
    if(fetch_int(args, "immediate", &immediate) != RMQC_OK)
        immediate = 0;

    if(fetch_str(args, "exchange", &exchange_name, &len) != RMQC_OK) {
        exchange = amqp_empty_bytes;
    }
    else {
        exchange.bytes = exchange_name;
        exchange.len = len;
    }

    if(fetch_str(args, "routing_key", &key_name, &len) != RMQC_OK) {
        routing_key = amqp_empty_bytes;
    }
    else {
        routing_key.bytes = key_name;
        routing_key.len = len;
    }

    if(fetch_str(args, "body", &body_str, &len) != RMQC_OK) {
        body = amqp_empty_bytes;
    }
    else {
        body.bytes = body_str;
        body.len = len;
    }

    idx = channel_index(self, channel);
    /* Once this connection uses publisher confirms, all publishing must use
     * confirm-mode channels. basic.return and the settlement methods arrive
     * asynchronously, and this client reads them synchronously from the
     * connection without a side queue for frames belonging to another
     * publishing channel: such a frame aborts the settlement wait. */
    if(any_confirm_channel(self) && !(idx >= 0 && self->confirm_mode[idx]))
        croak("send refused: this connection uses publisher confirms; publish on a confirm channel (channel %d is not)\n", channel);

    if(idx >= 0 && self->confirm_mode[idx]) {
        if(self->unusable)
            croak("send refused: a previous publication did not settle, so unread settlement frames may remain; discard this connection\n");
        if(fetch_int(args, "settle_timeout", &settle_timeout) != RMQC_OK || settle_timeout <= 0)
            settle_timeout = DEFAULT_SETTLE_TIMEOUT;
        clear_return(self);
        /* Claim the sequence number and mark the outcome unknown before the
         * write is attempted: amqp_basic_publish reports a local result only,
         * and a failure there is no proof that nothing reached the broker.
         * Only definite settlement in wait_settlement clears the flag. */
        self->next_tag[idx]++;
        self->unusable = 1;
        if(amqp_basic_publish(self->con, channel, exchange, routing_key, mandatory, immediate, &props, body))
            croak("could not send message\n");
        return wait_settlement(self, channel, self->next_tag[idx], settle_timeout);
    }

    if(amqp_basic_publish(self->con, channel, exchange, routing_key, mandatory, immediate, &props, body))
        croak("could not send message\n");

    return NULL;
}

/*
 * Put a channel into publisher-confirm mode (confirm.select). Refused on a
 * connection that has consumed: this implementation reads settlement frames
 * synchronously from the connection, so deliveries must not interleave with
 * them (rmqc_consume refuses symmetrically).
 */
extern int
rmqc_confirm_select(rmqc_t *self, HV *args)
{
    int channel, idx;

    if(self->con == NULL)
        croak("not connected\n");

    if(self->has_consumer)
        croak("confirm_select refused: this connection consumes; publisher-confirm connections must be publish-only\n");

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    /* Idempotent. The broker numbers publications from the FIRST confirm.select
     * on a channel and does not restart on a second one, so sending it again
     * and resetting our sequence would desynchronise the two. */
    idx = channel_index(self, channel);
    if(self->confirm_mode[idx])
        return RMQC_OK;

    amqp_confirm_select(self->con, channel);
    croak_on_amqp_error(amqp_get_rpc_reply(self->con), "confirm select");

    self->confirm_mode[idx] = 1;
    self->next_tag[idx] = 0;

    return RMQC_OK;
}

/*
 * multiple covers every outstanding tag up to delivery_tag, and delivery_tag 0
 * with multiple means all outstanding.
 */
static int
settles(uint64_t tag, uint64_t delivery_tag, int multiple)
{
    if(delivery_tag == tag)
        return 1;
    if(multiple && (delivery_tag == 0 || delivery_tag >= tag))
        return 1;
    return 0;
}

/*
 * Wait until the broker settles publication `tag` on `channel`, or the
 * deadline passes. Invariant: self->unusable is already set on entry and is
 * cleared only by a basic.ack or basic.nack that settles this tag. A
 * basic.return does not clear it; the publication is settled by the ack that
 * follows. Every other exit (timeout, close, protocol error) leaves it set.
 */
static const char *
wait_settlement(rmqc_t *self, int channel, uint64_t tag, int timeout_s)
{
    struct timespec start, now;
    struct timeval tv;
    amqp_frame_t frame;
    double elapsed, remaining;
    int returned = 0, st;

    clock_gettime(CLOCK_MONOTONIC, &start);

    for(;;) {
        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - start.tv_sec) + (now.tv_nsec - start.tv_nsec) / 1e9;
        remaining = timeout_s - elapsed;
        if(remaining <= 0)
            return "timeout";
        tv.tv_sec = (long) remaining;
        tv.tv_usec = (long) ((remaining - (double) tv.tv_sec) * 1e6);

        /* Frames decoded on the previous iteration are no longer referenced. */
        amqp_maybe_release_buffers(self->con);

        st = amqp_simple_wait_frame_noblock(self->con, &frame, &tv);
        if(st == AMQP_STATUS_TIMEOUT)
            return "timeout";   /* outcome unknown; self->unusable stays set */
        if(st != AMQP_STATUS_OK)
            croak("settlement wait: %s\n", amqp_error_string2(st));

        if(frame.frame_type == AMQP_FRAME_HEARTBEAT)
            continue;
        if(frame.frame_type != AMQP_FRAME_METHOD)
            croak("settlement wait: unexpected frame type %d on channel %d\n",
                  (int) frame.frame_type, (int) frame.channel);

        switch(frame.payload.method.id) {
        case AMQP_BASIC_ACK_METHOD: {
            amqp_basic_ack_t *a = (amqp_basic_ack_t *) frame.payload.method.decoded;
            if(frame.channel != channel)
                croak("settlement wait: basic.ack on unexpected channel %d\n", (int) frame.channel);
            if(settles(tag, a->delivery_tag, a->multiple)) {
                self->unusable = 0;
                return returned ? "returned" : "ack";
            }
            /* An ack for an older tag cannot be ours; keep waiting. */
            break;
        }
        case AMQP_BASIC_NACK_METHOD: {
            amqp_basic_nack_t *n = (amqp_basic_nack_t *) frame.payload.method.decoded;
            if(frame.channel != channel)
                croak("settlement wait: basic.nack on unexpected channel %d\n", (int) frame.channel);
            if(settles(tag, n->delivery_tag, n->multiple)) {
                self->unusable = 0;
                return "nack";
            }
            break;
        }
        case AMQP_BASIC_RETURN_METHOD: {
            amqp_basic_return_t *r = (amqp_basic_return_t *) frame.payload.method.decoded;
            amqp_message_t message;
            amqp_rpc_reply_t rr;
            size_t body_len;
            if(frame.channel != channel)
                croak("settlement wait: basic.return on unexpected channel %d\n", (int) frame.channel);
            /* basic.return precedes the confirm for a mandatory unroutable
             * publish and carries the whole message back (header + body
             * frames). Drain it before waiting for the ack, or those frames
             * would be read as the settlement. */
            rr = amqp_read_message(self->con, frame.channel, &message, 0);
            if(rr.reply_type != AMQP_RESPONSE_NORMAL)
                croak_on_amqp_error(rr, "read returned message");
            body_len = message.body.len;
            amqp_destroy_message(&message);
            record_return(self, r, body_len);
            returned = 1;
            break;
        }
        case AMQP_CONNECTION_BLOCKED_METHOD:
        case AMQP_CONNECTION_UNBLOCKED_METHOD:
            /* Resource alarms are not a settlement: keep waiting rather than
             * treating them as an unexpected frame. Defensive only, and not
             * exercised by the test suite: rabbitmq-c 0.11 does not advertise
             * the connection.blocked capability, so the broker never sends
             * these to this client. Kept so that a library version which does
             * advertise it tolerates an alarm instead of failing the publish. */
            break;
        case AMQP_CHANNEL_CLOSE_METHOD: {
            amqp_channel_close_t *m = (amqp_channel_close_t *) frame.payload.method.decoded;
            amqp_channel_close_ok_t ok;
            int code = m->reply_code;
            char text[256];
            snprintf(text, sizeof(text), "%.*s", (int) m->reply_text.len, (char *) m->reply_text.bytes);
            amqp_send_method(self->con, frame.channel, AMQP_CHANNEL_CLOSE_OK_METHOD, &ok);
            remove_channel(self, frame.channel);
            croak("settlement wait: server channel error %d, message: %s\n", code, text);
        }
        case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t *) frame.payload.method.decoded;
            amqp_connection_close_ok_t ok;
            int code = m->reply_code;
            char text[256];
            snprintf(text, sizeof(text), "%.*s", (int) m->reply_text.len, (char *) m->reply_text.bytes);
            amqp_send_method(self->con, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &ok);
            croak("settlement wait: server connection error %d, message: %s\n", code, text);
        }
        default:
            croak("settlement wait: unexpected method 0x%08X on channel %d\n",
                  (unsigned) frame.payload.method.id, (int) frame.channel);
        }
    }
}

static void
clear_return(rmqc_t *self)
{
    free(self->last_return.reply_text);
    free(self->last_return.exchange);
    free(self->last_return.routing_key);
    memset(&self->last_return, 0, sizeof(self->last_return));
}

static void
record_return(rmqc_t *self, amqp_basic_return_t *r, size_t body_len)
{
    clear_return(self);
    self->last_return.present = 1;
    self->last_return.reply_code = r->reply_code;
    self->last_return.reply_text = strndup((char *) r->reply_text.bytes, r->reply_text.len);
    self->last_return.exchange = strndup((char *) r->exchange.bytes, r->exchange.len);
    self->last_return.routing_key = strndup((char *) r->routing_key.bytes, r->routing_key.len);
    self->last_return.body_len = body_len;
}

/*
 * The basic.return of the most recent confirmed publication on this
 * connection, or undef. Never contains the body: only its length.
 */
extern SV *
rmqc_last_return(rmqc_t *self)
{
    HV *out;

    if(!self->last_return.present)
        return &PL_sv_undef;

    out = newHV();
    hv_store(out, "reply_code", strlen("reply_code"), newSViv(self->last_return.reply_code), 0);
    hv_store(out, "reply_text", strlen("reply_text"), newSVpv(self->last_return.reply_text, 0), 0);
    hv_store(out, "exchange", strlen("exchange"), newSVpv(self->last_return.exchange, 0), 0);
    hv_store(out, "routing_key", strlen("routing_key"), newSVpv(self->last_return.routing_key, 0), 0);
    hv_store(out, "body_len", strlen("body_len"), newSVuv(self->last_return.body_len), 0);
    return newRV_noinc((SV *) out);
}

extern int
rmqc_send_ack(rmqc_t *self, HV *args)
{
    int channel, multiple = 0;
    unsigned long delivery_tag = 0;

    if(self->con == NULL)
        croak("not connected\n");

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    fetch_int(args, "multiple", &multiple);
    fetch_uint(args, "delivery_tag", &delivery_tag);

    if(amqp_basic_ack(self->con, channel, delivery_tag, multiple))
        croak("could not send ack for %lu\n", delivery_tag);

    return RMQC_OK;
}

extern int
rmqc_consume(rmqc_t *self, HV *args)
{
    amqp_bytes_t queue, consumer_tag;
    char *queue_name = NULL, *tag_name = NULL;
    int channel, no_local, no_ack, exclusive, len, i;

    if(self->con == NULL)
        croak("not connected\n");

    for(i = 0; i < self->num_channels; i++)
        if(self->confirm_mode[i])
            croak("consume refused: channel %d is in publisher-confirm mode; publisher-confirm connections must be publish-only\n",
                  self->channels[i]);

    if(fetch_int(args, "channel", &channel) != RMQC_OK)
        channel = DEFAULT_CHANNEL;

    if(!channel_exists(self, channel)) {
        amqp_channel_open(self->con, channel);
        croak_on_amqp_error(amqp_get_rpc_reply(self->con), "channel open");
        store_channel(self, channel);
    }

    if(fetch_int(args, "no_local", &no_local) != RMQC_OK)
        no_local = DEFAULT_NO_LOCAL;

    if(fetch_int(args, "no_ack", &no_ack) != RMQC_OK)
        no_ack = DEFAULT_NO_ACK;

    if(fetch_int(args, "exclusive", &exclusive) != RMQC_OK)
        exclusive = DEFAULT_EXCLUSIVE;

    if(fetch_str(args, "consumer_tag", &tag_name, &len) != RMQC_OK) {
        consumer_tag = amqp_empty_bytes;
    }
    else {
        consumer_tag.bytes = tag_name;
        consumer_tag.len = len;
    }

    if(fetch_str(args, "queue", &queue_name, &len) != RMQC_OK) {
        queue = amqp_empty_bytes;
    }
    else {
        queue.bytes = queue_name;
        queue.len = len;
    }

    amqp_basic_consume(self->con, channel, queue, consumer_tag, no_local,
                       no_ack, exclusive, amqp_empty_table);
    croak_on_amqp_error(amqp_get_rpc_reply(self->con), "consume");
    self->has_consumer = 1;

    return RMQC_OK;
}

extern SV
*rmqc_receive(rmqc_t *self, HV *args)
{
    long unsigned int timeout = 0;
    struct timeval *tval_p = NULL, tval;
    amqp_envelope_t envelope;
    amqp_rpc_reply_t ret;
    HV *out = newHV();
    SV *out_ref = &PL_sv_undef;

    if(self->con == NULL)
        croak("not connected\n");

    if(fetch_uint(args, "timeout", &timeout) == RMQC_OK) {
        memset(&tval, 0, sizeof(tval));
        tval.tv_sec = timeout;
        tval_p = &tval;
    }

    amqp_maybe_release_buffers(self->con);
    ret = amqp_consume_message(self->con, &envelope, tval_p, 0);
    if(ret.reply_type == AMQP_RESPONSE_NORMAL) {
        hv_store(out, "channel", strlen("channel"),
                 newSViv(envelope.channel), 0);
        hv_store(out, "delivery_tag", strlen("delivery_tag"),
                 newSViv(envelope.delivery_tag), 0);
        hv_store(out, "redelivered", strlen("redelivered"),
                 newSViv(envelope.redelivered), 0);
        hv_store(out, "exchange", strlen("exchange"),
                 newSVpv(envelope.exchange.bytes, envelope.exchange.len), 0);
        hv_store(out, "consumer_tag", strlen("consumer_tag"),
                 newSVpv(envelope.consumer_tag.bytes, envelope.consumer_tag.len), 0);
        hv_store(out, "routing_key", strlen("routing_key"),
                 newSVpv(envelope.routing_key.bytes, envelope.routing_key.len), 0);
        hv_store(out, "body", strlen("body"),
                 newSVpv(envelope.message.body.bytes, envelope.message.body.len), 0);

        amqp_destroy_envelope(&envelope);
        out_ref = newRV_noinc((SV *) out);
    }
    else if(ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION
        && ret.library_error == AMQP_STATUS_TIMEOUT)
    {
        return out_ref;
    }
    else {
        croak_on_amqp_error(ret, "consume_message");
    }

    return out_ref;
}

extern int
rmqc_close(rmqc_t *self)
{
    if(self && self->con) {
        /* rmqc_close_channel removes the channel from the list, so iterate
         * from the end: indexing forwards skips every channel that shifts
         * down into a position already passed, leaving them in channels[]
         * for a later reconnect to mistake for open channels. */
        while(self->num_channels > 0)
            rmqc_close_channel(self, self->channels[self->num_channels - 1]);

        croak_on_amqp_error(amqp_connection_close(self->con, AMQP_REPLY_SUCCESS), "close");
        amqp_destroy_connection(self->con);
        self->con = NULL;

        /* Connection-lifetime state, reset only on a successful close so that
         * reconnecting this object starts clean. The per-channel arrays need
         * nothing: the loop above drained them, and store_channel initialises
         * each position when a channel is reopened.
         *
         * unusable is deliberately NOT reset. An indeterminate confirmed
         * publication is terminal for the object: reconnecting must not
         * rehabilitate it, or "discard the connection" would stop meaning
         * anything. It is only ever clear here anyway, since a clean
         * lifecycle settles every publication. */
        self->has_consumer = 0;
        clear_return(self);
    }

    return RMQC_OK;
}

extern int
rmqc_close_channel(rmqc_t *self, int channel)
{
    croak_on_amqp_error(amqp_channel_close(self->con, channel, AMQP_REPLY_SUCCESS), "channel close");
    remove_channel(self, channel);

    return RMQC_OK;
}

extern int
rmqc_destroy(rmqc_t *self)
{
    if(self != NULL) {
        if(self->con) {
            amqp_destroy_connection(self->con);
            self->con = NULL;
        }
        free(self->host);
        free(self->user);
        free(self->pass);
        free(self->vhost);
        free(self->channels);
        self->channels = NULL;
        free(self->confirm_mode);
        free(self->next_tag);
        clear_return(self);
        if(self->cacert)
            free(self->cacert);
        free(self);
    }

    return RMQC_OK;
}

static int
channel_exists(rmqc_t *self, int channel)
{
    int i;

    for(i = 0; i < self->num_channels; i++)
        if(channel == self->channels[i])
            return 1;

    return 0;
}

static int
any_confirm_channel(rmqc_t *self)
{
    int i;

    for(i = 0; i < self->num_channels; i++)
        if(self->confirm_mode[i])
            return 1;

    return 0;
}

static int
channel_index(rmqc_t *self, int channel)
{
    int i;

    for(i = 0; i < self->num_channels; i++)
        if(channel == self->channels[i])
            return i;

    return -1;
}

static void
store_channel(rmqc_t *self, int channel)
{
    if(channel_exists(self, channel))
        return;

    if(self->num_channels == self->max_channels)
        croak("max configured channels of %d exceeded\n", self->max_channels);

    self->confirm_mode[self->num_channels] = 0;
    self->next_tag[self->num_channels] = 0;
    self->channels[self->num_channels++] = channel;
}

static void
remove_channel(rmqc_t *self, int channel)
{
    int i, j;

    if(!channel_exists(self, channel) || self->num_channels == 0)
        return;

    for(i = 0; i < self->num_channels; i++) {
        if(self->channels[i] == channel)
            break;
    }

    /* Shift all channels after i one place to the left, with their state. */
    for(j = i + 1; j < self->num_channels; i++, j++) {
        self->channels[i] = self->channels[j];
        self->confirm_mode[i] = self->confirm_mode[j];
        self->next_tag[i] = self->next_tag[j];
    }

    self->num_channels--;
}

static int
fetch_int(HV *h, char *key, int *val)
{
    SV **v;

    if(!hv_exists(h, key, strlen(key))
       || !(v = hv_fetch(h, key, strlen(key), 0)))
    {
        return RMQC_ERR;
    }

    *val = SvIV(*v);
    return RMQC_OK;
}

static int
fetch_uint(HV *h, char *key, unsigned long *val)
{
    SV **v;

    if(!hv_exists(h, key, strlen(key))
       || !(v = hv_fetch(h, key, strlen(key), 0)))
    {
        return RMQC_ERR;
    }

    *val = SvUV(*v);
    return RMQC_OK;
}

static int
fetch_str(HV *h, char *key, char **val, int *len)
{
    SV **v;
    STRLEN plen;

    if(!hv_exists(h, key, strlen(key))
       || !(v = hv_fetch(h, key, strlen(key), 0)))
    {
        return RMQC_ERR;
    }

    *val = SvPV(*v, plen);
    *len = plen;

    return RMQC_OK;
}

static void
croak_on_amqp_error(amqp_rpc_reply_t x, char const *context)
{
    switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return;

    case AMQP_RESPONSE_NONE:
        croak("%s: missing RPC reply type!\n", context);
        break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        croak("%s: %s\n", context, amqp_error_string2(x.library_error));
        break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (x.reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
            croak("%s: server connection error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
            break;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
            amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
            croak("%s: server channel error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
            break;
        }
        default:
            croak("%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
            break;
        }
        break;
    }
}
