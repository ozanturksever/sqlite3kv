.PHONY: all
all: validate test clean

## Run validates
.PHONY: validate
validate:
	#golangci-lint run

## Run tests
.PHONY: test
test:
	sleep 5
	go test -v -race ${TEST_ARGS} ./...

## Clean local data
.PHONY: clean
clean:
	$(RM) goverage.report $(shell find . -type f -name *.out)
