"""
Шаг 1: Генерация синонимов брендов через Together.ai API
Если файл synonyms/all_brand_synonyms.json существует, пропускаем генерацию.
"""
import asyncio
import json
import aiohttp
import pandas as pd
from pathlib import Path
import time
from utils import (
    TOGETHER_API_KEY, API_URL, MODEL,
    BRANDS_FILE, SYNONYMS_FILE, SYNONYMS_DIR,
    ensure_dirs
)

# Настройки (уменьшенное количество запросов)
MAX_RPS = 4  # Половина от обычного (было 8)
BATCH_SIZE = 50
MAX_RETRIES = 5
RETRY_DELAY = 3
BATCH_OUTPUT_DIR = SYNONYMS_DIR / "batches"

# Промпт для генерации синонимов
SYSTEM_PROMPT = """Ты эксперт по брендам и торговым маркам. Твоя задача - сгенерировать все возможные варианты написания бренда, которые могут встретиться в транскрибированных диалогах."""

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "original": {"type": "string", "description": "Оригинальное название бренда"},
                    "exact_variants": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Точные варианты написания (регистр, пробелы)"
                    },
                    "phonetic_variants": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Фонетические варианты (транслитерация, произношение)"
                    },
                    "colloquial_variants": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Разговорные варианты (сокращения, жаргон)"
                    }
                },
                "required": ["original", "exact_variants", "phonetic_variants", "colloquial_variants"]
            }
        }
    },
    "required": ["items"]
}


class RateLimiter:
    def __init__(self, max_per_second: float):
        self.min_interval = 1.0 / max_per_second
        self.last_call = 0

    async def acquire(self):
        now = time.time()
        time_since_last = now - self.last_call
        if time_since_last < self.min_interval:
            await asyncio.sleep(self.min_interval - time_since_last)
        self.last_call = time.time()


async def generate_synonyms_for_brand(
    session: aiohttp.ClientSession,
    brand: str,
    rate_limiter: RateLimiter,
    retry_count: int = 0
) -> dict:
    """Генерирует синонимы для одного бренда"""
    await rate_limiter.acquire()

    headers = {
        "Authorization": f"Bearer {TOGETHER_API_KEY}",
        "Content-Type": "application/json"
    }

    user_message = f"""Бренд: {brand}

Сгенерируй все возможные варианты написания этого бренда:
1. exact_variants: варианты с разным регистром, пробелами, дефисами
2. phonetic_variants: как может звучать при произношении, транслитерация RU<->EN
3. colloquial_variants: разговорные сокращения, жаргон"""

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "brand_synonyms",
                "schema": OUTPUT_SCHEMA,
                "strict": True
            }
        },
        "temperature": 0.3
    }

    try:
        async with session.post(API_URL, headers=headers, json=payload, timeout=60) as response:
            if response.status in [429, 503] and retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (2 ** retry_count)  # Exponential backoff
                print(f"  [RETRY {retry_count + 1}] {brand} - status {response.status}, wait {wait_time}s")
                await asyncio.sleep(wait_time)
                return await generate_synonyms_for_brand(session, brand, rate_limiter, retry_count + 1)

            response.raise_for_status()
            result = await response.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "{}")
            parsed = json.loads(content)

            return {
                "original_brand": brand,
                "status": "success",
                "response": parsed
            }

    except Exception as e:
        if retry_count < MAX_RETRIES:
            wait_time = RETRY_DELAY * (2 ** retry_count)
            print(f"  [RETRY {retry_count + 1}] {brand} - {str(e)[:50]}, wait {wait_time}s")
            await asyncio.sleep(wait_time)
            return await generate_synonyms_for_brand(session, brand, rate_limiter, retry_count + 1)

        return {
            "original_brand": brand,
            "status": "error",
            "error": str(e)
        }


async def process_batch(
    session: aiohttp.ClientSession,
    brands: list,
    batch_num: int,
    rate_limiter: RateLimiter
) -> list:
    """Обрабатывает батч брендов"""
    print(f"\n[BATCH {batch_num}] Обработка {len(brands)} брендов...")

    tasks = [
        generate_synonyms_for_brand(session, brand, rate_limiter)
        for brand in brands
    ]
    results = await asyncio.gather(*tasks)

    success = sum(1 for r in results if r["status"] == "success")
    errors = sum(1 for r in results if r["status"] == "error")
    print(f"[BATCH {batch_num}] Успешно: {success}, Ошибок: {errors}")

    # Сохранение батча
    BATCH_OUTPUT_DIR.mkdir(exist_ok=True)
    batch_file = BATCH_OUTPUT_DIR / f"batch_{batch_num:04d}.json"
    with open(batch_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


async def main():
    """Главная функция генерации синонимов"""
    ensure_dirs()

    # Проверка существования файла
    if SYNONYMS_FILE.exists():
        print(f"Файл синонимов уже существует: {SYNONYMS_FILE}")
        print("Пропускаем генерацию.")
        return

    # Загрузка брендов
    if not BRANDS_FILE.exists():
        raise FileNotFoundError(f"Файл брендов не найден: {BRANDS_FILE}")

    df = pd.read_csv(BRANDS_FILE, header=None, names=["brand"])
    brands = df["brand"].dropna().unique().tolist()

    # Фильтруем бренды длиннее 3 символов
    brands = [b for b in brands if len(str(b).strip()) > 3]
    print(f"Брендов для обработки: {len(brands)}")

    # Разбивка на батчи
    batches = []
    for i in range(0, len(brands), BATCH_SIZE):
        batches.append(brands[i:i + BATCH_SIZE])

    print(f"Батчей: {len(batches)}")
    print(f"Примерное время: {len(brands) / MAX_RPS / 60:.1f} минут")

    # Обработка
    rate_limiter = RateLimiter(MAX_RPS)
    all_results = []
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        for batch_num, batch in enumerate(batches):
            results = await process_batch(session, batch, batch_num, rate_limiter)
            all_results.extend(results)

            # Прогресс
            elapsed = time.time() - start_time
            processed = (batch_num + 1) * BATCH_SIZE
            if processed > 0:
                remaining = (len(brands) - processed) / (processed / elapsed)
                print(f"  Прогресс: {min(processed, len(brands))}/{len(brands)}, осталось ~{remaining/60:.1f} мин")

    # Сохранение итогового файла
    with open(SYNONYMS_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    success_count = sum(1 for r in all_results if r["status"] == "success")
    print(f"\n{'='*60}")
    print(f"ГОТОВО")
    print(f"Успешно: {success_count}/{len(all_results)}")
    print(f"Время: {(time.time() - start_time)/60:.1f} мин")
    print(f"Результат: {SYNONYMS_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
