
clean:
	go clean -i .
	rm -rf vendor

test:
	go test -cover .

deps:
	go get -d -v .

# Requires proto3
protoc:
	protoc net.proto --go_out=plugins=grpc:.

flatc:
	rm -rf fbtypes/*.go
	flatc -g fbtypes/fbtypes.fbs
