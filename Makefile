SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
DOCKER_COMPOSE := docker compose

include .env

help:
	@echo -e "\033[0;32m Usage: make [target] "
	@echo
	@echo -e "\033[1m targets:\033[0m"
	@egrep '^(.+):*\ ##\ (.+)' ${MAKEFILE_LIST} | column -t -c 2 -s ':#'
.PHONY: help

<---docker------->: ## -----------------------------------------------------------------------
start: ## Start all services (redis, rabbitmq, kafka) and wait until ready
	$(DOCKER_COMPOSE) up -d --wait redis rabbitmq kafka
	@echo "✓ All services are ready!"
.PHONY: start

stop: ## Stop and remove all containers, networks and volumes
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
.PHONY: stop

shell: ## Run a shell inside the container
	$(DOCKER_COMPOSE) run --rm -it phpcli sh
.PHONY: shell

<---composer----->: ## -----------------------------------------------------------------------
install: ## Run composer install
	$(DOCKER_COMPOSE) run --rm phpcli composer install --no-cache
.PHONY: install

update: ## run composer update
	$(DOCKER_COMPOSE) run --rm -it -e XDEBUG_MODE=off phpcli composer update

autoload: ## Run composer dump-autoload
	$(DOCKER_COMPOSE) run --rm phpcli composer dumpautoload
.PHONY: autoload

<---qa tools----->: ## -----------------------------------------------------------------------
phpunit: phpunit-unit phpunit-integration ## Run all tests (unit + integration)
.PHONY: phpunit

phpunit-unit: ## Run unit tests only
	$(DOCKER_COMPOSE) run --rm phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php /app/tests/unit
.PHONY: phpunit-unit

phpunit-integration: start ## Run integration tests only (requires: make start)
	$(DOCKER_COMPOSE) run --rm phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php /app/tests/integration
.PHONY: phpunit-integration

phpunit-reports: start ## Run all tests with reports (requires: make start)
	$(DOCKER_COMPOSE) run --rm -e PCOV_ENABLED=1 phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php /app/tests --coverage-clover tests/reports/clover.xml --coverage-xml tests/reports/coverage-xml
.PHONY: phpunit-reports

phpunit-coverage: start ## Run all tests with coverage text (requires: make start)
	$(DOCKER_COMPOSE) run --rm -e PCOV_ENABLED=1 phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php /app/tests --coverage-text
.PHONY: phpunit-coverage

phpunit-coverage-html: start ## Run all tests with HTML coverage (requires: make start)
	$(DOCKER_COMPOSE) run --rm -e PCOV_ENABLED=1 phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php /app/tests --coverage-html tests/reports/coverage-html
.PHONY: phpunit-coverage-html

phpstan: ## Run PHPStan analysis (requires: make start)
	$(DOCKER_COMPOSE) run --rm phpcli vendor/bin/phpstan analyse /app/src -c phpstan.neon
.PHONY: phpstan

phpcs: ## Run coding standards (requires: make start)
	$(DOCKER_COMPOSE) run --rm phpcli vendor/bin/phpcs /app/src
.PHONY: phpcs

<---ssh -------->: ## -----------------------------------------------------------------------
ssh-agent: ## Get SSH agent ready
	eval `ssh-agent -s`
	ssh-add
.PHONY: ssh-agent
