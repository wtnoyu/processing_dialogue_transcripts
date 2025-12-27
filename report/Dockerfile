FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY scripts/ ./scripts/
COPY brand_normalizer.py .

# Создание директорий
RUN mkdir -p data output synonyms

# Точка входа
ENTRYPOINT ["python", "brand_normalizer.py"]
