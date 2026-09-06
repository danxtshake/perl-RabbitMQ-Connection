# RabbitMQ::Connection

A thin Perl XS client over [librabbitmq-c](https://github.com/alanxz/rabbitmq-c):
connect, declare, bind, publish, consume, ack. Version 0.9 adds **publisher
confirms** with correct `mandatory` (return-then-ack) handling; see the POD in
`lib/RabbitMQ/Connection.pm` for the API and its delivery semantics.

    perl Makefile.PL && make && make test      # tests skip without TEST_HOST
    xt/docker-test.sh                         # full run against a throwaway broker

This is a fork; the confirm extension lives here first (`Changes`).
