# Сборка приложений (сидер, оба продюсера, классификатор, веб-UI)
docker compose build db-init producer-valid producer-fraud classifier web-ui
docker compose config --services

# Поднять только инфраструктуру
docker compose up -d postgres kafka kafka-ui
docker compose ps

# Разово создать схему и наполнить БД (идемпотентно)
docker compose --profile seed run --rm db-init

# Запустить генератор валидных транзакций
docker compose up -d producer-valid
docker compose logs -f producer-valid

# Запустить генератор мошеннических транзакций (без валидного)
docker compose stop producer-valid
docker compose up -d producer-fraud
docker compose logs -f producer-fraud

docker compose stop producer-valid
docker compose rm -f -s producer-valid

docker compose stop producer-fraud
docker compose rm -f -s producer-fraud

# Запуск классификатора и веб-сайта
docker compose up -d classifier web-ui
Start-Process http://127.0.0.1:3000
docker compose logs -f classifier