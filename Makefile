PWD := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

SOURCES := $(shell find -name '*.go')
GRADER_FILES = $(shell find root/grader -type f)
RUNNER_FILES = $(shell find root/runner -type f)
BINARIES := bin/x86_64/grader bin/x86_64/benchmark bin/x86_64/runner bin/x86_64/validator bin/armhf/benchmark bin/armhf/runner
GRADER_VOLUMES ?= --volume=$(PWD)/run/grader/log:/var/log/omegaup --volume=$(PWD)/run/grader/runtime:/var/lib/omegaup --volume=$(PWD)/run/grader/conf:/etc/omegaup/grader --volume=$(PWD)/cmd/grader/data:/data
RUNNER_VOLUMES ?= --volume=$(PWD)/run/runner/log:/var/log/omegaup --volume=$(PWD)/run/runner/runtime:/var/lib/omegaup --volume=$(PWD)/run/runner/conf:/etc/omegaup/runner

default: $(BINARIES)

$(BINARIES): bin/.binary-stamp
	touch $(BINARIES)

bin/.stamp:
	mkdir -p $@
	touch $@

bin/.network-stamp: bin/.stamp
	docker network create quark
	touch $@

bin/.prebuild-stamp: Dockerfile.prebuild bin/.stamp
	docker build --force-rm --rm=true -t omegaup/quark-prebuild -f ./Dockerfile.prebuild .
	touch $@

bin/.minijail-download-stamp: bin/.stamp
	wget https://s3.amazonaws.com/omegaup-minijail/minijail-xenial-distrib-x86_64.tar.bz2 \
		-O bin/minijail-xenial-distrib-x86_64.tar.bz2
	touch $@

bin/.minijail-stamp: Dockerfile.minijail bin/.minijail-download-stamp
	docker build --force-rm --rm=true -t omegaup/minijail -f ./Dockerfile.minijail .
	touch $@

bin/.binary-stamp: $(SOURCES) bin/.prebuild-stamp Dockerfile.build
	rm -rf bin/x86_64 bin/armhf
	mkdir -p bin/x86_64 bin/armhf
	docker build --force-rm --rm=true -t omegaup/quark-build -f ./Dockerfile.build .
	$(eval CONTAINER=$(shell docker create omegaup/quark-build))
	docker cp $(CONTAINER):/go/bin/common_test bin/x86_64/common_test
	docker cp $(CONTAINER):/go/bin/runner_test bin/x86_64/runner_test
	docker cp $(CONTAINER):/go/bin/grader_test bin/x86_64/grader_test
	docker cp $(CONTAINER):/go/bin/benchmark bin/x86_64/benchmark
	docker cp $(CONTAINER):/go/bin/runner bin/x86_64/runner
	docker cp $(CONTAINER):/go/bin/grader bin/x86_64/grader
	docker cp $(CONTAINER):/go/bin/sudo bin/x86_64/sudo
	docker cp $(CONTAINER):/go/bin/validator bin/x86_64/validator
	docker cp $(CONTAINER):/go/bin/common_test-armhf bin/armhf/common_test
	docker cp $(CONTAINER):/go/bin/runner_test-armhf bin/armhf/runner_test
	docker cp $(CONTAINER):/go/bin/benchmark-armhf bin/armhf/benchmark
	docker cp $(CONTAINER):/go/bin/runner-armhf bin/armhf/runner
	docker rm -v $(CONTAINER)
	touch $@

bin/.grader-stamp: $(BINARIES) Dockerfile.grader $(GRADER_FILES)
	docker build --force-rm --rm=true -t omegaup/grader -f ./Dockerfile.grader .
	touch $@

bin/.runner-stamp: $(BINARIES) bin/.minijail-stamp Dockerfile.runner $(RUNNER_FILES)
	# TODO(lhchavez): Dynamically detect minijail's dependencies instead of
	# manually copying them.
	cp -L /lib/x86_64-linux-gnu/libcap.so.2 root/runner/lib/x86_64-linux-gnu/libcap.so.2
	cp -L /lib/x86_64-linux-gnu/libdl.so.2 root/runner/lib/x86_64-linux-gnu/libdl.so.2
	cp -L /lib/x86_64-linux-gnu/librt.so.1 root/runner/lib/x86_64-linux-gnu/librt.so.1
	cp -L /lib/x86_64-linux-gnu/libc.so.6 root/runner/lib/x86_64-linux-gnu/libc.so.6
	cp -L /lib/x86_64-linux-gnu/libpthread.so.0 root/runner/lib/x86_64-linux-gnu/libpthread.so.0
	cp -L /lib/x86_64-linux-gnu/libnss_nis.so.2 root/runner/lib/x86_64-linux-gnu/libnss_nis.so.2
	cp -L /lib/x86_64-linux-gnu/libnss_files.so.2 root/runner/lib/x86_64-linux-gnu/libnss_files.so.2
	cp -L /lib64/ld-linux-x86-64.so.2 root/runner/lib64/ld-linux-x86-64.so.2
	docker build --force-rm --rm=true -t omegaup/runner -f ./Dockerfile.runner .
	touch $@

.PHONY: grader
grader: bin/.grader-stamp bin/.network-stamp
	docker run --rm=true --net=quark --name=quark --publish=11302:11302 $(GRADER_VOLUMES) omegaup/grader $(GRADER_FLAGS)

.PHONY: runner
runner: bin/.runner-stamp bin/.network-stamp
	docker run --privileged --rm=true --net=quark $(RUNNER_VOLUMES) omegaup/runner $(RUNNER_FLAGS)
