BLUE=\033[0;34m
NC=\033[0m # No Color

.PHONY: infra-up infra-ps infra-down install run-batch run-api run test-dep sleep test clean

infra-up:
	@echo "\n${BLUE}Starting the infrastructure...${NC}\n"
	@docker-compose -f docker-compose.infra.yml up -d

infra-ps:
	@echo "\n${BLUE}Querying the infrastructure...${NC}\n"
	@docker-compose -f docker-compose.infra.yml ps

infra-down:
	@echo "\n${BLUE}Stopping the infrastructure...${NC}\n"
	@docker-compose -f docker-compose.infra.yml down

install:
	@pip install -r requirements.txt

run-batch:
	@python -m etl

run-api:
	@flask run

test-dep:
	@export FLASK_ENV=development; pytest 

sleep:
	@sleep 5

test: infra-up sleep test-dep infra-down

run: infra-up sleep run-batch run-api infra-down

clean:
	@echo "\n${BLUE}Cleaning up...${NC}\n"
	@rm -rf .pytest_cache __pycache__ tests/__pycache__ tests/.pytest_cache etl/__pycache__ etl/.pytest_cache etl/*.pyc .coverage .coverage.* htmlcov
