build: clean
	go build -o kscanner cmd/main/main.go

clean:
	rm -f kscanner

tarball: clean build
	tar zcvf kscanner-$$(date --iso-8601).tar.gz kscanner README.md

run:
	go run cmd/main/main.go