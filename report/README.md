# Brand Normalizer

Алгоритм нормализации брендов в транскрибированных диалогах.

## Структура проекта

```
report/
├── data/                     # Входные данные
│   ├── brands.csv           # Справочник брендов
│   └── dialogs.csv          # Диалоги для обработки
├── output/                   # Результаты (создается автоматически)
│   ├── result.csv
│   ├── metrics.json
│   ├── low_precision_brands.csv
│   └── report.xlsx
├── synonyms/                 # Кэш синонимов
│   └── all_brand_synonyms.json
├── scripts/                  # Скрипты пайплайна
│   ├── utils.py
│   ├── 01_generate_synonyms.py
│   ├── 02_match_synonyms.py
│   ├── 03_filter_llm.py
│   └── 04_generate_report.py
├── brand_normalizer.py       # Главный скрипт
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── PIPELINE.md              # Описание бизнес-логики
└── README.md
```

## Анализ исходного датасета

### Покрытие справочником

| Метрика | Значение |
|---------|----------|
| Брендов в справочнике (`brands.csv`) | 11,258 |
| Уникальных брендов в ground truth | 274 |
| Найдено в справочнике | 113 (41.2%) |
| **Не найдено в справочнике** | **161 (58.8%)** |

### Ключевые находки

**Потолок Recall ограничен ~41%** — почти 60% брендов из эталонной разметки отсутствуют в справочнике.

**Топ-10 отсутствующих брендов (по частоте упоминаний):**

| Бренд | Упоминаний |
|-------|------------|
| ВсеИнструменты | 64 |
| Mail.ru | 9 |
| WhatsApp | 8 |
| Инструменты.ру | 4 |
| 3M | 4 |
| Совкомбанк | 3 |
| Сбер | 3 |
| Wildberries | 2 |
| Леруа Мерлен | 2 |
| СДЭК | 2 |

**Примечание:** Бренд "ВсеИнструменты" (64 упоминания) отсутствует, но "ВсеИнструменты.ру" есть в справочнике — это указывает на проблему нормализации в ground truth.

### Рекомендации

Для улучшения метрик:
1. Дополнить `brands.csv` недостающими брендами (особенно высокочастотными)
2. Добавить синонимы для вариаций написания (например, "ВсеИнструменты" → "ВсеИнструменты.ру")
3. Рассмотреть открытый поиск LLM без привязки к справочнику

## Быстрый старт

### Подготовка данных

1. Поместите файлы в директорию `data/`:
   - `brands.csv` — справочник брендов (одна колонка, без заголовка)
   - `dialogs.csv` — диалоги с колонками:
     - `source_text_index` — ID диалога
     - `source_text` — текст диалога
     - `extracted_brands` — JSON с эталонными брендами (для валидации)

2. Создайте `.env` файл в корне проекта (на уровень выше):
   ```
   TOGETHER_API_KEY=your_api_key_here
   ```

### Запуск через Python

```bash
# Установка зависимостей
pip install -r requirements.txt

# Полный пайплайн
python brand_normalizer.py

# Только определенный шаг
python brand_normalizer.py --step 1  # Генерация синонимов
python brand_normalizer.py --step 2  # Матчинг
python brand_normalizer.py --step 3  # LLM фильтрация
python brand_normalizer.py --step 4  # Генерация отчета

# Пропустить генерацию синонимов (использовать существующий файл)
python brand_normalizer.py --skip-synonyms
```

### Запуск через Docker

```bash
# Сборка образа
docker-compose build

# Полный пайплайн
docker-compose run brand-normalizer

# Только генерация синонимов
docker-compose run generate-synonyms

# Пайплайн без генерации синонимов
docker-compose run run-pipeline
```

## Выходные файлы

### result.csv
Детализированные результаты по каждому найденному бренду:
- `dialog_id` — ID диалога
- `original_text` — цитата из диалога
- `detected_brand` — обнаруженный текст
- `normalized_brand` — нормализованное название
- `confidence` — уверенность (0.0-1.0)

### metrics.json
Агрегированные метрики:
```json
{
  "precision": 0.5026,
  "recall": 0.3980,
  "f1": 0.4442,
  "true_positives": 195,
  "false_positives": 193,
  "false_negatives": 295
}
```

### low_precision_brands.csv
Топ-10 брендов с наихудшим Precision (для анализа ошибок).

### report.xlsx
Excel-отчет с двумя листами:
- Metrics — агрегированные метрики
- Details — сравнение ground truth и предсказаний по каждому диалогу

## Настройка

### Параметры скриптов

В `scripts/utils.py`:
- `MODEL` — модель LLM (по умолчанию `openai/gpt-oss-120b`)

В `scripts/01_generate_synonyms.py`:
- `MAX_RPS` — максимум запросов в секунду (4)
- `BATCH_SIZE` — размер батча (50)

В `scripts/03_filter_llm.py`:
- `MAX_CONCURRENT` — максимум параллельных запросов (8)
- `TIMEOUT` — таймаут запроса в секундах (180)
- `MAX_RETRIES` — количество повторных попыток (2)

## Требования

- Python 3.9+
- Together.ai API ключ
- ~10GB RAM для обработки больших справочников
