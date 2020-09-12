FROM docker.pkg.github.com/omegaup/omegajail/omegajail-builder-rootfs-runtime:v3.7.0 AS base

FROM base AS builder

# Install karel.js in a builder container to avoid leaving a lot of trash in
# the /root directory.
RUN mkdir /tmp/karel && \
    echo '{"dependencies":{"karel":"1.1.0"}}' > /tmp/karel/package.json && \
    (cd /tmp/karel && /opt/nodejs/bin/npm install) && \
    mv /tmp/karel/node_modules/* /opt/nodejs/lib/node_modules/ && \
    rm -rf /tmp/karel

FROM base

COPY --from=builder /opt/nodejs/lib/node_modules/ /opt/nodejs/lib/node_modules/
RUN mkdir -p /etc/omegaup/runner /var/lib/omegaup
RUN chmod 777 /var/lib/omegaup
COPY bin/omegaup-runner /usr/bin/omegaup-runner
COPY config.json /etc/omegaup/runner/config.json

RUN useradd ubuntu --uid 1000 --shell /bin/bash
RUN chown -R 1000:1000 /var/lib/omegaup/ /src
USER 1000
WORKDIR /src

ENTRYPOINT ["/usr/bin/omegaup-runner"]
