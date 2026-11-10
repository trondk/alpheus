.PHONY: all clean spec-status lint ci check-changelog generate proto check-generated check-deps test test-crd test-chart test-templates cli test-s3 test-agent-s3 vet images e2e e2e-minio test-upgrade fuzz bench bench-etl release verify-release vulndb vuln deploy

all: generate vet check-deps test

# Deepcopy functions and the CRD from the kubebuilder markers in api/, and
# the controller's RBAC role from those in internal/controller. The Helm
# chart gets a copy of both, the Python template one of the component
# contract, the website the CLI reference (WWW-4), the list of tests from
# their doc comments and the changes from CHANGELOG.md, and alpheus ui its
# help page, which is docs/ui.md.
generate:
	go tool controller-gen object:headerFile=hack/boilerplate.go.txt paths=./api/...
	go tool controller-gen crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	go tool controller-gen rbac:roleName=alpheus-controller paths=./internal/controller/... output:rbac:artifacts:config=config/rbac
	cp config/crd/bases/alpheus-dev.org_streamruns.yaml charts/alpheus/files/crd.yaml
	cp config/rbac/role.yaml charts/alpheus/files/role.yaml
	cp proto/component.proto templates/component-python/alpheus/component.proto
	go run ./hack/clireference > docs/cli.md
	go run ./hack/testlist > docs/tests.md
	go run ./hack/changelog docs > docs/changes.md
	cp docs/ui.md internal/cli/ui/help.md
	$(MAKE) proto

# Go code for proto/ in internal/pb, with buf and the generators in go.mod.
proto:
	buf generate

# Fails when generated code is not up to date (GO-63).
check-generated: generate
	git diff --exit-code -- api config internal/pb charts/alpheus/files templates/component-python/alpheus/component.proto docs/cli.md docs/tests.md docs/changes.md internal/cli/ui/help.md

vet:
	go vet ./...

# golangci-lint with .golangci.yml, at the version pinned in
# hack/golangci-lint.mod, kept apart from go.mod so its dependencies do not
# change the binaries' (GO-59). It checks gofmt too.
lint:
	go tool -modfile=hack/golangci-lint.mod golangci-lint run ./...

# What CI runs, and fails on any finding: vet, lint, generated code that
# is up to date, the dependency rule and the tests (GO-59, GO-63).
ci: vet lint check-generated check-deps check-changelog test

# Fails unless CHANGELOG.md has an Unreleased section and every release
# section has its three parts (GO-65).
check-changelog:
	go run ./hack/changelog check

# Deletes what the build leaves in _out/: binaries, image digests, the
# example's files, measurements, test reports, releases and the
# vulnerability database. The test cluster's keys in vagrant/_out/ stay.
clean:
	rm -rf _out

# The alpheus CLI, for this machine, in _out/bin.
cli:
	go build -o _out/bin/alpheus ./cmd/alpheus

# The controller's tests run against kube-apiserver and etcd from envtest,
# which setup-envtest downloads the first time.
ENVTEST_K8S := 1.37.0
test:
	KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S) -p path)" go test -race ./...

# Checks the CRD's schema and CEL rules against the cluster in KUBECONFIG,
# for example the Vagrant test cluster (source vagrant/credentials.sh).
test-crd: generate
	hack/test-crd.sh

# Lints and renders the Helm chart with helm (HELM), and checks what it
# renders for the registry, digests and the hub.
test-chart: generate
	hack/test-chart.sh

# Runs the Python template and the log-report example in local mode in
# containers (podman or docker); the Go template's test runs with make test.
test-templates:
	templates/component-python/test.sh
	examples/log-report/test.sh

# Runs internal/store's tests against Garage in the Vagrant test cluster,
# over HTTPS through the gateway, trusting the test CA.
VAGRANT := $(abspath vagrant)
GARAGE_ENV = . $(VAGRANT)/_out/secrets.env && \
	ALPHEUS_TEST_S3_ENDPOINT=https://$$($(VAGRANT)/talos.sh ip):30900 \
	ALPHEUS_TEST_S3_BUCKET=alpheus-data \
	ALPHEUS_TEST_S3_REGION=garage \
	ALPHEUS_TEST_S3_ACCESS_KEY=$$GARAGE_DEFAULT_ACCESS_KEY \
	ALPHEUS_TEST_S3_SECRET_KEY=$$GARAGE_DEFAULT_SECRET_KEY \
	ALPHEUS_TEST_S3_CA_FILE=$(VAGRANT)/_out/ca.crt
test-s3:
	$(GARAGE_ENV) go test -race -count=1 -run TestS3 -v ./internal/store

# Runs agents and components in-process against Garage, as testcase03's
# pipeline with signals through the builtin hub and a restart.
test-agent-s3:
	$(GARAGE_ENV) go test -race -count=1 -run TestS3 -v ./internal/agent

# Builds the agent, the controller and the test components as images and
# pushes them to Zot in the Vagrant test cluster (_out/images.env).
images:
	hack/images.sh

# Installs the controller in the Vagrant test cluster with the Helm chart and
# those images, and runs the testcases in test/e2e against it. Afterwards
# a report of the run is served on port 30718 of the control plane
# (hack/report.sh); REPORT=0 skips it.
e2e:
	hack/e2e.sh

# The same testcases against MinIO instead of Garage: installs a MinIO for
# the run and removes it afterwards (hack/e2e-minio.sh).
e2e-minio:
	hack/e2e-minio.sh

# Runs testcase01, testcase02 and testcase03, with the images from the last make e2e,
# to see how the system performs. BENCH_* change their sizes (see
# test/e2e/e2e_test.go).
bench:
	SKIP_IMAGES=1 hack/e2e.sh -run 'TestTestcase0[1-3]$$'

# Runs each fuzz test for FUZZTIME: the parsers of what the agent, the
# controller and alpheus replay read from storage (GO-58). A failing input
# is saved under the package's testdata/fuzz/ and then runs in make test.
FUZZTIME ?= 1m
FUZZ = ./internal/chunk:FuzzParseManifest ./internal/chunk:FuzzParseSegment \
	./internal/chunk:FuzzParseAcks ./internal/replay:FuzzParse
fuzz:
	@set -e; for t in $(FUZZ); do \
	  echo "fuzz $${t#*:} for $(FUZZTIME)"; \
	  go test $${t%%:*} -run '^$$' -fuzz "^$${t#*:}$$" -fuzztime $(FUZZTIME) | tail -1; \
	done

# Installs an older commit's chart and images on the test cluster and runs
# testcase18, which upgrades to this commit's in the middle of a run
# (GO-66). UPGRADE_FROM is the commit or tag; see hack/test-upgrade.sh.
test-upgrade:
	hack/test-upgrade.sh

# A release of the tagged commit in _out/release/<version>: binaries,
# images, the chart, SBOMs, SHA256SUMS and signatures (SPEC 14). See
# hack/release.sh for its settings.
release:
	hack/release.sh

# A copy of the Go vulnerability database in _out/vulndb, for make vuln
# and the release. VULNDB_URL may point at a mirror of vulndb.zip.
VULNDB ?= _out/vulndb
VULNDB_URL ?= https://vuln.go.dev/vulndb.zip
vulndb:
	rm -rf $(VULNDB) $(VULNDB).zip && mkdir -p $(VULNDB)
	curl -fsSL -o $(VULNDB).zip $(VULNDB_URL)
	unzip -q -d $(VULNDB) $(VULNDB).zip && rm $(VULNDB).zip
	@sed 's/.*"modified":"\([^"]*\)".*/vulnerability database from \1/' $(VULNDB)/index/db.json ; echo

# Known vulnerabilities in code Alpheus calls, from the copy in $(VULNDB)
# and without the network (GO-11).
vuln:
	@test -f $(VULNDB)/index/db.json || { echo "no vulnerability database in $(VULNDB); run make vulndb"; exit 1; }
	go tool govulncheck -db file://$(abspath $(VULNDB)) ./...

# Checks a release with its public key: make verify-release VERSION=vX.Y.Z
verify-release:
	hack/verify-release.sh _out/release/$(VERSION)

# Builds the images and installs the chart on the test cluster, with the
# UI on port 30717 of the VM, without running the tests.
deploy:
	DEPLOY_ONLY=1 hack/e2e.sh

# The flagship benchmark: large-file ETL with examples/meter-etl, with
# export, split and clean at 1, 2, 4 and 8 replicas each, and a table of
# the results (testcase08).
bench-etl:
	hack/bench-etl.sh

# Alpheus never reads a chunk's content, so it must not depend on Parquet or
# Arrow (GO-101).
check-deps:
	@if go list -deps ./... | grep -q '^github.com/apache/arrow-go'; then \
		echo "arrow-go is a dependency (GO-101)"; exit 1; fi

# The status of SPEC.md's requirements: labels counted, and every fail.
spec-status:
	hack/spec-status.sh
