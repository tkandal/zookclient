all:	$(wildcard *.go)
	dep ensure -v
	go build -v .

clean:
	rm -rf vendor

vet:	$(wildcard *.go)
	@go vet $(wildcard *.go)

lint:	$(wildcard *.go)
	@golint $(wildcard *.go)


.PHONY:	all

