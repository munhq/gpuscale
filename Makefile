IMG ?= ghcr.io/munhq/gpuscale-controller:latest
NODE_IMG ?= ghcr.io/munhq/gpuscale-node:latest

.PHONY: build test fmt vet lint docker-build docker-push generate manifests install run clean

# Build the controller binary
build: fmt vet
	go build -o bin/gpuscale-controller ./cmd/controller/

# Run tests
test:
	go test ./... -v -count=1

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Run golangci-lint
lint:
	golangci-lint run ./...

# Generate deepcopy methods and CRD manifests
generate:
	controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./api/..."
	controller-gen crd paths="./api/..." output:crd:artifacts:config=config/crd

# Generate CRD manifests only
manifests:
	controller-gen crd paths="./api/..." output:crd:artifacts:config=config/crd
	controller-gen rbac:roleName=gpuscale-controller paths="./internal/controller/..." output:rbac:artifacts:config=config/rbac

# Install CRDs into the cluster
install: manifests
	kubectl apply -f config/crd/

# Run the controller locally (outside cluster)
run: build
	./bin/gpuscale-controller

# Build controller Docker image
docker-build:
	docker build -t $(IMG) -f docker/controller/Dockerfile .

# Build node Docker image
docker-build-node:
	docker build -t $(NODE_IMG) -f docker/node/Dockerfile .

# Push images
docker-push:
	docker push $(IMG)

docker-push-node:
	docker push $(NODE_IMG)

# Build and push all images
docker-all: docker-build docker-build-node docker-push docker-push-node

# Install Helm chart
helm-install:
	helm install gpuscale charts/gpuscale/ -n gpuscale-system --create-namespace

# Upgrade Helm chart
helm-upgrade:
	helm upgrade gpuscale charts/gpuscale/ -n gpuscale-system

# Uninstall Helm chart
helm-uninstall:
	helm uninstall gpuscale -n gpuscale-system

# Clean build artifacts
clean:
	rm -rf bin/

# Tidy dependencies
tidy:
	go mod tidy
