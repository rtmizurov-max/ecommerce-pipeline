# E-commerce ETL Pipeline

Production-ready ETL пайплайн с PostgreSQL и Grafana аналитикой. Полностью контейнеризован с Docker.

## Возможности

- **Extract**: Загрузка данных из REST API с retry логикой
- **Transform**: Генерация пользовательских событий (view → cart → purchase)
- **Load**: Идемпотентная запись в БД с обработкой конфликтов
- **Analytics**: Готовые Grafana дашборды с метриками конверсии
- **Infrastructure**: Полная докеризация, запуск одной командой

## Технологический стек

- **Python 3.11** — ETL логика
- **PostgreSQL 15** — Хранилище данных
- **Grafana OSS** — Аналитика и визуализация
- **Docker Compose** — Оркестрация
- **SQLAlchemy** — ORM для БД
- **Pandas** — Трансформация данных

## Быстрый старт

```bash
git clone https://github.com/rtmizurov-max/ecommerce-pipeline.git
cd ecommerce-pipeline
cp .env.example .env
docker compose up --build
```

Grafana доступна на `http://localhost:3000` (логин:admin/пароль:admin).
При переходе по ссылке :
1. **Перейти в меню Connections**
2. **Выбрать Data Sources**
3. **Открыть источник PostgreSQL**
4. **Пролистать страницу вниз**
5. **Нажать кнопку Save & Test**

После успешной проверки соединения:
1. **Перейти в раздел Dashboards**
2. **Открыть единственный доступный дашборд**
3. **Нажать кнопку Refresh**
4. **После обновления отобразятся все метрики.**

## Архитектура

```
┌─────────────┐      ┌──────────────┐      ┌────────────┐
│  Fake Store │─────▶│  ETL Pipeline│─────▶│ PostgreSQL │
│     API     │ HTTP │   (Python)   │ SQL  │            │
└─────────────┘      └──────────────┘      └─────┬──────┘
                                                  │
                                                  ▼
                                            ┌──────────┐
                                            │  Grafana │
                                            │ Analytics│
                                            └──────────┘
```

## Схема данных

**products** — Каталог товаров с рейтингами  
**events** — Воронка поведения пользователей (просмотры, добавления в корзину, покупки)

Оптимизировано индексами по `event_date`, `user_id`, `event_type`

## Поток данных

1. **Fetch** — Загрузка товаров и корзин из API
2. **Transform** — Преобразование в  события воронки с метаданными
3. **Load** — Запись в PostgreSQL с upsert логикой
4. **Visualize** — Отображение метрик конверсии в Grafana

## Разработка

```bash
# Пересборка после изменений кода
docker compose up --build

# Ручной запуск ETL
docker compose run --rm etl

# Просмотр логов
docker compose logs -f etl

# Доступ к БД
psql postgresql://postgres:postgres@localhost:5432/ecommerce
```

## Структура проекта

```
├── src/
│   ├── config.py       # Конфигурация и логирование
│   ├── fetcher.py      # API клиент с retry
│   ├── transformer.py  # Генерация воронки событий
│   ├── loader.py       # Операции с БД
│   └── pipeline.py     # Оркестрация
├── grafana/
│   ├── provisioning/   # Автонастройка datasources
│   └── dashboards/     # Готовая аналитика
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Ключевые метрики

- **Total Revenue ($)**: Общая выручка за выбранный период
- **Total Orders**: Количество уникальных заказов (по сессиям)
- **Conversion Rate (%)**: Доля сессий с покупкой от общего числа просмотров
- **Average Order Value ($)**: Средний чек (выручка / количество заказов)
- **Daily Revenue Trend**: Динамика выручки по дням
- **Revenue by Category**: Сравнение выручки по категориям товаров
- **Top Products by Revenue**: Рейтинг товаров по выручке, количеству покупок и просмотрам