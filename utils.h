#ifndef XS_UTILS_H
#define XS_UTILS_H

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "utils.h"

#include <stdint.h>
#include <amqp_ssl_socket.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

#define RMQC_OK  0
#define RMQC_ERR 1

/*
 * A returned (unroutable) mandatory publication, as reported by basic.return.
 * The body is drained and discarded; only its length is kept.
 */
struct rmqc_return {
    int present;
    int reply_code;
    char *reply_text;
    char *exchange;
    char *routing_key;
    size_t body_len;
};

/*
 * Publisher-confirm model:
 *   - confirm state and sequence numbers are per channel;
 *   - settlement reading is synchronous and connection-wide;
 *   - at most one confirmed publication awaits settlement at a time;
 *   - an unknown outcome poisons the connection until it is discarded.
 */
struct rmqc {
    amqp_connection_state_t con;
    char *host;
    int port;
    char *vhost;
    char *user;
    char *pass;
    int *channels;          /* open channel numbers, by position           */
    int *confirm_mode;      /* per position: channel is in confirm mode    */
    uint64_t *next_tag;     /* per position: last sequence number assigned
                               on that confirm channel (not yet settled)  */
    int max_channels;
    int num_channels;
    int has_consumer;       /* basic.consume was issued on this connection;
                               stays set, it is not a "currently consuming"
                               flag but a publish-only disqualification     */
    int unusable;           /* connection-wide: a publication's outcome is
                               unknown, so confirmed sends are refused      */
    struct rmqc_return last_return;
    int ssl;
    int heartbeat;
    int verify;
    char *cacert;
};

typedef struct rmqc rmqc_t;
typedef rmqc_t * RabbitMQ__Connection;

extern int rmqc_new(rmqc_t **self, HV *args);

extern int rmqc_destroy(rmqc_t *self);

extern int rmqc_declare_exchange(rmqc_t *self, HV *args);

extern char *rmqc_declare_queue(rmqc_t *self, HV *args);

extern int rmqc_bind(rmqc_t *self, HV *args);

extern int rmqc_consume(rmqc_t *self, HV *args);

extern SV *rmqc_receive(rmqc_t *self, HV *args);

extern int rmqc_connect(rmqc_t *self);

extern const char *rmqc_send(rmqc_t *self, HV *args);

extern int rmqc_confirm_select(rmqc_t *self, HV *args);

extern SV *rmqc_last_return(rmqc_t *self);

extern int rmqc_send_ack(rmqc_t *self, HV *args);

extern int rmqc_close_channel(rmqc_t *self, int channel);

extern int rmqc_close(rmqc_t *self);

#endif
