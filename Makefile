run:
	@./bin/baobab
build:
	@go build -race -o bin/baobab
test:
	@go test ./...

