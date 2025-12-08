PAGE ?= 0

run:
	@./bin/baobab
build:
	@go build -race -o bin/baobab
test:
	@go test ./...
inspect-page:
	@go run cmd/pager/main.go $(PAGE)
