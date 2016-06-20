SOURCES := $(shell find -name '*.go')
BINARIES := bin/grader bin/runner bin/validator

default: $(BINARIES)

$(BINARIES): bin/.binary-stamp
	touch $@

bin/.stamp:
	mkdir -p $@
	touch $@

bin/.prebuild-stamp: Dockerfile.prebuild bin/.stamp
	docker build --force-rm --rm=true -t omegaup/quark-prebuild -f ./Dockerfile.prebuild .
	touch $@

bin/.binary-stamp: $(SOURCES) bin/.prebuild-stamp Dockerfile.build
	docker build --force-rm --rm=true -t omegaup/quark-build -f ./Dockerfile.build .
	$(eval CONTAINER=$(shell docker create omegaup/quark-build))
	docker cp $(CONTAINER):/go/bin/grader bin/grader
	docker cp $(CONTAINER):/go/bin/runner bin/runner
	docker cp $(CONTAINER):/go/bin/runner bin/validator
	docker rm -v $(CONTAINER)
	touch $@

bin/.grader-stamp: $(BINARIES) Dockerfile.grader root/grader
	docker build --force-rm --rm=true -t omegaup/grader -f ./Dockerfile.grader .
	touch $@

.PHONY: grader
grader: bin/.grader-stamp
	docker run --rm=true --publish=11302:11302 $(GRADER_VOLUMES) omegaup/grader $(GRADER_FLAGS)
