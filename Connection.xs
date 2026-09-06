#define PERL_NO_GET_CONTEXT
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "utils.h"

MODULE = RabbitMQ::Connection		PACKAGE = RabbitMQ::Connection		
    
PROTOTYPES: ENABLE

SV *
_new(package, args_ref)
    char *package
    SV *args_ref
    INIT:
        rmqc_t *s;
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        rmqc_new(&s, args);
        SV *addr = newSViv(PTR2IV(s));
        SV * const self = newRV_noinc(addr);
        RETVAL = sv_bless(self, gv_stashpv(package, 0));
    OUTPUT:
        RETVAL

void
connect(self)
    RabbitMQ::Connection self
    CODE:
        rmqc_connect(self);

char *
_declare_queue(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        RETVAL = rmqc_declare_queue(self, args);
    OUTPUT:
        RETVAL

void
_declare_exchange(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        rmqc_declare_exchange(self, args);

SV *
_send(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
        const char *status;
    CODE:
        status = rmqc_send(self, args);
        RETVAL = newSVpv(status, 0);
    OUTPUT:
        RETVAL

void
_confirm_select(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        rmqc_confirm_select(self, args);

SV *
last_return(self)
    RabbitMQ::Connection self
    CODE:
        RETVAL = rmqc_last_return(self);
    OUTPUT:
        RETVAL

int
is_blocked(self)
    RabbitMQ::Connection self
    CODE:
        RETVAL = rmqc_is_blocked(self);
    OUTPUT:
        RETVAL

void
_send_ack(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        rmqc_send_ack(self, args);

void
_bind(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        rmqc_bind(self, args);

void
_consume(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        rmqc_consume(self, args);

SV *
_receive(self, args_ref)
    RabbitMQ::Connection self
    SV *args_ref
    INIT:
        HV *args = (HV *) SvRV(args_ref);
    CODE:
        RETVAL = rmqc_receive(self, args);
    OUTPUT:
        RETVAL

void
close_channel(self, channel)
    RabbitMQ::Connection self
    int channel
    CODE:
        rmqc_close_channel(self, channel);

void
close(self)
    RabbitMQ::Connection self
    CODE:
        rmqc_close(self);

void
DESTROY(self)
    RabbitMQ::Connection self
    CODE:
        rmqc_destroy(self);
