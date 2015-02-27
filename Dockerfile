FROM debian:wheezy

ADD https://github.com/anope/anope/releases/download/2.0.1/anope-2.0.1-source.tar.gz /tmp/
ADD modules /tmp/modules

RUN apt-get update && \
    apt-get install -y build-essential cmake libssl-dev libssl1.0.0 openssl libgnutls-dev libpq-dev ruby && \
    cd /tmp && \
    tar -xzf *.tar.gz && \
    cd anope-* && \
    mv modules/extra/m_ssl_gnutls.cpp modules/ && \
    mv /tmp/modules/m_pgsql.cpp modules/ && \
    printf "INSTDIR=\"/srv/services\"\nRUNGROUP=\"\"\nUMASK=077\nDEBUG=\"no\"\nUSE_RUN_CC_PL=\"no\"\nUSE_PCH=\"no\"\nEXTRA_INCLUDE_DIRS=\"/usr/include/postgresql\"\nEXTRA_LIB_DIRS=\"\"\nEXTRA_CONFIG_ARGS=\"\"" > config.cache && \
    ./Config -quick && \
    cd build && \
    make && make install && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /srv/services/conf/*

ADD . /tmp/

RUN cp -avr /tmp/conf /srv/services/ && \
    cp -avr /tmp/bin /usr/local/ && \
    mkdir /srv/certs &&\
    rm -rf  /tmp/*

VOLUME ["/srv/certs/"]

ENTRYPOINT ["entrypoint"]
CMD ["/srv/services/bin/services", "--nofork"]
