.PHONY: build test lint up down logs ps loadtest tidy

build:
	go build ./...

test:
	go test ./... -race

tidy:
	go mod tidy

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

# Requires k6 (https://k6.io) installed locally; see loadtest/ingest.js.
loadtest:
	k6 run loadtest/ingest.js
