

prep:
	@which protoc || { echo "protoc: command not found!"; exit 1; }
	@which glide || { echo "glide: command not found!"; exit 1; }

test:
	go test -cover ./...

cov:
	go test -coverprofile=/tmp/coverage.out
	go tool cover -html=/tmp/coverage.out

# Requires proto3
protoc:
	protoc net.proto --go_out=plugins=grpc:.
