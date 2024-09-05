build: clean
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 CC=musl-gcc \
    go build -tags musl -ldflags='-extldflags=-static' -o kscanner-linux-amd64-musl ./cmd/main/main.go

clean:
	rm -f kscanner

tarball: clean build
	tar zcvf kscanner-$$(date --iso-8601).tar.gz kscanner-linux-amd64-musl README.md

run:
	go run cmd/main/main.go