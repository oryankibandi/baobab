PAGE ?= 0
BIN_PATH="./bin/baobab"
DATA_FILE="data"
FREE_LIST="data_fl"
WAL_PATH="bb.wal"
CONFIG_PATH="bb_config"

run:
	@[ -e $(BIN_PATH) ] && rm $(BIN_PATH) || echo ""
	$(MAKE) build
	@$(BIN_PATH)
run-clean:
	@[ -e $(BIN_PATH) ] && rm $(BIN_PATH) || echo ""
	@[ -e $(DATA_FILE) ] && rm $(DATA_FILE) || echo ""
	@[ -e $(FREE_LIST) ] && rm $(FREE_LIST) || echo ""
	@[ -e $(WAL_PATH) ] && rm $(WAL_PATH) || echo ""
	@[ -e $(CONFIG_PATH) ] && rm $(CONFIG_PATH) || echo ""
	$(MAKE) build
	@$(BIN_PATH)
build:
	@echo "🏗️ building binary...."
	@go build -race -o $(BIN_PATH)
	@echo "✅ built binary at $(BIN_PATH)"
test:
	@go test ./...
inspect-page:
	@go run cmd/pager/main.go $(PAGE)
