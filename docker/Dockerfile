FROM docker.pkg.github.com/omegaup/omegajail/omegajail-builder-rootfs-runtime:v3.2.0

RUN mkdir -p /etc/omegaup/runner /var/lib/omegaup
COPY bin/omegaup-runner /usr/bin/omegaup-runner
COPY config.json /etc/omegaup/runner/config.json

RUN useradd ubuntu --uid 1000 --shell /bin/bash
RUN chown -R 1000:1000 /var/lib/omegaup/
USER 1000
WORKDIR /src

ENTRYPOINT ["/usr/bin/omegaup-runner"]
