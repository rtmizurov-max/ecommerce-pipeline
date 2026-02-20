# E-commerce ETL Pipeline

Production-ready ETL пайплайн с PostgreSQL и Grafana аналитикой. Полностью контейнеризован с Docker.

## Возможности

- **Extract**: Загрузка данных из REST API с retry логикой
- **Transform**: Генерация аналитической воронки событий (view → cart → purchase)
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
git clone https://github.com/YOUR_USERNAME/ecommerce-etl.git
cd ecommerce-etl
cp .env.example .env
docker-compose up --build
```

Grafana доступна на `http://localhost:3000` (логин:admin/пароль:admin).
При переходе по ссылке небходимо ввести логин и пароль, затем прейти на вкладку Connections и выбрать "View configured data sources",
там отобразится бд PostgreSQL, необходимо нажать на нее и пролистать страницу вниз,а затем нажать кнопку "Save & Test". После всех выполненных дейтсвий переходим на вкладку "Dashboards" и отркываем единственный дашборд
,нажава на кнопку "Refresh", все виджеты обновятся, и вы увидите ключевые показатели.

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
docker-compose up --build

# Ручной запуск ETL
docker-compose run --rm etl

# Просмотр логов
docker-compose logs -f etl

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