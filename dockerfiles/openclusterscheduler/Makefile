# This Makefile builds and runs a container image.

IMAGE_NAME = $(shell basename $(CURDIR))
IMAGE_TAG = latest
CONTAINER_NAME = $(IMAGE_NAME)

.PHONY: build
build:
	@echo "Building the go-drmaa Open Cluster Scheduler image..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: run
run: build
	@echo "Running the container..."
	mkdir -p ./installation
	docker run -it -h master --name $(CONTAINER_NAME) -v ./installation:/opt/cs-install $(IMAGE_NAME):$(IMAGE_TAG) /bin/bash

.PHONY: clean
clean:
	@echo "Removing the container..."
	docker rm -f $(CONTAINER_NAME) || true
	@echo "Removing the image..."
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true
	@echo "Removing the installation directory..."
	rm -rf ./installation
