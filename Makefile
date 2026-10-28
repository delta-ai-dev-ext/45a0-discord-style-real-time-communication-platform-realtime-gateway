SERVICE_NAME ?= realtime_gateway

.PHONY: dev test build run lint typecheck

dev:
	bun --watch src/index.ts

test:
	bun test --coverage

build:
	docker build -t $(SERVICE_NAME):latest .

run:
	bun run src/index.ts

lint:
	bun x eslint src/ tests/

typecheck:
	bun node_modules/typescript/bin/tsc --noEmit
