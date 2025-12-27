"""
Шаг 3: Верификация брендов через LLM (Together.ai)
Фильтрует кандидатов, оставляя только реальные упоминания брендов.
"""
import asyncio
import json
import aiohttp
import pandas as pd
from pathlib import Path
import time
from utils import (
    TOGETHER_API_KEY, API_URL, MODEL, OUTPUT_DIR, ensure_dirs
)

# Входной файл (результат шага 2)
MATCHED_FILE = OUTPUT_DIR / "matched_candidates.csv"
# Выходной файл
VERIFIED_FILE = OUTPUT_DIR / "verified_brands.csv"

# Настройки
MAX_CONCURRENT = 8  # Максимум параллельных запросов
MAX_RETRIES = 2  # 3 попытки всего (1 + 2 retry)
RETRY_DELAY = 3
TIMEOUT = 180  # секунд (первая попытка)
RETRY_TIMEOUT = 300  # секунд (при retry)

# Промпт
SYSTEM_PROMPT = """Ты эксперт аналитик. Твоя задача - определить, какие бренды/компании из предоставленного списка ДЕЙСТВИТЕЛЬНО упоминаются в диалоге КАК НАЗВАНИЯ БРЕНДОВ.

Нас интересуют ТОЛЬКО бренды: товаров, производителей, маркетплейсов.

КРИТИЧЕСКИ ВАЖНО:
1. Бренд считается упомянутым ТОЛЬКО если в контексте диалога он используется КАК НАЗВАНИЕ КОМПАНИИ/ПРОДУКТА/БРЕНДА
2. Включай ТОЛЬКО если понятно, что говорят о конкретной компании/продукте
3. Для каждого найденного бренда укажи ТОЧНЫЕ цитаты из диалога
4. Уверенность (0.0-1.0): насколько уверен, что это именно бренд
5. Не обрабатывай е-мейл адреса, имена собственные, наименования технологий
6. Если в списке есть сокращенное И полное наименование - цитаты с полным НЕ дублируй в сокращенное"""


def create_output_schema(brand_list: list) -> dict:
    """Создает JSON schema с enum из списка брендов"""
    return {
        "type": "object",
        "properties": {
            "brands": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "enum": brand_list},
                        "quotes": {"type": "array", "items": {"type": "string"}},
                        "confidence": {"type": "number"}
                    },
                    "required": ["name", "quotes", "confidence"],
                    "additionalProperties": False
                }
            }
        },
        "required": ["brands"],
        "additionalProperties": False
    }


def create_open_schema() -> dict:
    """Создает схему без enum - для поиска любых брендов"""
    return {
        "type": "object",
        "properties": {
            "brands": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "quotes": {"type": "array", "items": {"type": "string"}},
                        "confidence": {"type": "number"}
                    },
                    "required": ["name", "quotes", "confidence"],
                    "additionalProperties": False
                }
            }
        },
        "required": ["brands"],
        "additionalProperties": False
    }


async def verify_brands(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    dialog: dict,
    progress: dict
) -> dict:
    """Верифицирует бренды через LLM с семафором для ограничения параллельности"""
    dialog_id = dialog["dialog_id"]
    dialog_text = dialog["text"]
    candidates = dialog["candidates"]

    async with semaphore:
        for attempt in range(MAX_RETRIES + 1):
            try:
                headers = {
                    "Authorization": f"Bearer {TOGETHER_API_KEY}",
                    "Content-Type": "application/json"
                }

                # Формируем запрос в зависимости от наличия кандидатов
                if candidates:
                    brands_formatted = []
                    brand_names = []
                    for item in candidates:
                        if '|' in item:
                            brand, synonym = item.split('|', 1)
                            brands_formatted.append(f"- {brand} (упоминается как '{synonym}')")
                            brand_names.append(brand)
                        else:
                            brands_formatted.append(f"- {item}")
                            brand_names.append(item)

                    user_message = f"""ДИАЛОГ:
{dialog_text}

СПИСОК БРЕНДОВ ДЛЯ ПРОВЕРКИ:
{chr(10).join(brands_formatted)}

Укажи, какие бренды из списка ДЕЙСТВИТЕЛЬНО упоминаются в диалоге."""

                    schema = create_output_schema(brand_names)
                else:
                    user_message = f"""ДИАЛОГ:
{dialog_text}

Найди ВСЕ бренды товаров, производителей, маркетплейсов, которые упоминаются в диалоге."""

                    schema = create_open_schema()

                payload = {
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_message}
                    ],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {"name": "brand_filter", "schema": schema, "strict": True}
                    },
                    "temperature": 0.1
                }

                # Увеличенный timeout при retry
                current_timeout = TIMEOUT if attempt == 0 else RETRY_TIMEOUT
                timeout = aiohttp.ClientTimeout(total=current_timeout)
                async with session.post(API_URL, headers=headers, json=payload, timeout=timeout) as response:
                    if response.status in [429, 503]:
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                            continue
                        raise Exception(f"API error: {response.status}")

                    response.raise_for_status()
                    result = await response.json()

                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "{}")
                    parsed = json.loads(content)

                    # Фильтруем confidence=0
                    brands = [b for b in parsed.get("brands", []) if b.get("confidence", 0) > 0]

                    # Обновляем прогресс
                    progress["success"] += 1
                    progress["done"] += 1
                    if progress["done"] % 10 == 0:
                        print(f"  Прогресс: {progress['done']}/{progress['total']} (успешно: {progress['success']})")

                    return {
                        "dialog_id": dialog_id,
                        "verified_brands": brands,
                        "ground_truth": dialog["ground_truth"],
                        "source_text": dialog_text,
                        "status": "success"
                    }

            except Exception as e:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue

                progress["errors"] += 1
                progress["done"] += 1
                if progress["done"] % 10 == 0:
                    print(f"  Прогресс: {progress['done']}/{progress['total']} (ошибок: {progress['errors']})")

                return {
                    "dialog_id": dialog_id,
                    "error": str(e),
                    "ground_truth": dialog["ground_truth"],
                    "source_text": dialog_text,
                    "status": "error"
                }


async def main():
    """Главная функция LLM-фильтрации"""
    ensure_dirs()

    print("=" * 60)
    print("ШАГ 3: ВЕРИФИКАЦИЯ ЧЕРЕЗ LLM")
    print("=" * 60)

    # Загрузка кандидатов
    if not MATCHED_FILE.exists():
        raise FileNotFoundError(f"Файл кандидатов не найден: {MATCHED_FILE}")

    df = pd.read_csv(MATCHED_FILE)
    print(f"Диалогов: {len(df)}")

    # Подготовка - обрабатываем ВСЕ диалоги
    dialogs = []
    for idx, row in df.iterrows():
        if pd.notna(row["matched_brands"]) and row["matched_brands"]:
            candidates = [b.strip() for b in row["matched_brands"].split("\n") if b.strip()]
        else:
            candidates = []

        dialogs.append({
            "dialog_id": row["dialog_id"],
            "text": row["source_text"],
            "candidates": candidates,
            "ground_truth": row["ground_truth"]
        })

    dialogs_with_candidates = sum(1 for d in dialogs if d["candidates"])
    print(f"Диалогов с кандидатами: {dialogs_with_candidates}/{len(dialogs)}")
    print(f"Параллельных запросов: {MAX_CONCURRENT}")
    print(f"Timeout: {TIMEOUT}s (retry: {RETRY_TIMEOUT}s), Retries: {MAX_RETRIES}")

    # Прогресс
    progress = {"done": 0, "total": len(dialogs), "success": 0, "errors": 0}

    # Истинная асинхронность с семафором
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    start_time = time.time()

    print(f"\nЗапуск {len(dialogs)} задач параллельно...")

    async with aiohttp.ClientSession() as session:
        tasks = [
            verify_brands(session, semaphore, dialog, progress)
            for dialog in dialogs
        ]
        results = await asyncio.gather(*tasks)

    # Формирование результата
    output_rows = []
    for r in results:
        if r["status"] == "success":
            verified = r.get("verified_brands", [])
            output_rows.append({
                "dialog_id": r["dialog_id"],
                "source_text": r["source_text"],
                "ground_truth": r["ground_truth"],
                "verified_brands": json.dumps(verified, ensure_ascii=False),
                "verified_count": len(verified)
            })

    output_df = pd.DataFrame(output_rows)
    output_df.to_csv(VERIFIED_FILE, index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print("ГОТОВО")
    print(f"{'='*60}")
    print(f"Время: {(time.time() - start_time)/60:.1f} мин")
    print(f"Успешно: {progress['success']}/{progress['total']}")
    print(f"Ошибок: {progress['errors']}")
    print(f"Результат: {VERIFIED_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
