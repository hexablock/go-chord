
clean:
	go clean -i .
	rm -rf vendor

prep:
	@which protoc || { echo "protoc: command not found!"; exit 1; }

deps:
	go get -d -v ./...

test:
	go test -cover ./...

cov:
	go test -coverprofile=/tmp/coverage.out
	go tool cover -html=/tmp/coverage.out

# Requires proto3
protoc:
	protoc coordinate/structs.proto --go_out=plugins=grpc:.
	protoc net.proto -I ./ -I ../../../ --go_out=plugins=grpc:.
