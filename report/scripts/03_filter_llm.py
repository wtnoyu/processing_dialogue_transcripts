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
BATCH_SIZE = 10
MAX_RPS = 8
MAX_RETRIES = 3
RETRY_DELAY = 2

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


class RateLimiter:
    def __init__(self, max_per_second: float):
        self.min_interval = 1.0 / max_per_second
        self.last_call = 0

    async def acquire(self):
        now = time.time()
        if now - self.last_call < self.min_interval:
            await asyncio.sleep(self.min_interval - (now - self.last_call))
        self.last_call = time.time()


async def verify_brands(
    session: aiohttp.ClientSession,
    dialog_id: int,
    dialog_text: str,
    candidates: list,
    rate_limiter: RateLimiter,
    retry_count: int = 0
) -> dict:
    """Верифицирует бренды через LLM"""
    # Если нет кандидатов - возвращаем пустой результат без вызова API
    if not candidates:
        return {
            "dialog_id": dialog_id,
            "verified_brands": [],
            "status": "success"
        }

    await rate_limiter.acquire()

    headers = {
        "Authorization": f"Bearer {TOGETHER_API_KEY}",
        "Content-Type": "application/json"
    }

    # Формируем список брендов с синонимами
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

    try:
        async with session.post(API_URL, headers=headers, json=payload, timeout=60) as response:
            if response.status in [429, 503] and retry_count < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
                return await verify_brands(session, dialog_id, dialog_text, candidates, rate_limiter, retry_count + 1)

            response.raise_for_status()
            result = await response.json()

            content = result.get("choices", [{}])[0].get("message", {}).get("content", "{}")
            parsed = json.loads(content)

            # Фильтруем confidence=0
            brands = [b for b in parsed.get("brands", []) if b.get("confidence", 0) > 0]

            return {
                "dialog_id": dialog_id,
                "verified_brands": brands,
                "status": "success"
            }

    except Exception as e:
        if retry_count < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY * (retry_count + 1))
            return await verify_brands(session, dialog_id, dialog_text, candidates, rate_limiter, retry_count + 1)

        return {
            "dialog_id": dialog_id,
            "error": str(e),
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
    print(f"Примерное время: {len(dialogs) / MAX_RPS / 60:.1f} мин")

    # Обработка
    rate_limiter = RateLimiter(MAX_RPS)
    results = []
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(dialogs), BATCH_SIZE):
            batch = dialogs[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE

            print(f"\n[BATCH {batch_num}] Обработка {len(batch)} диалогов...")

            tasks = [
                verify_brands(session, d["dialog_id"], d["text"], d["candidates"], rate_limiter)
                for d in batch
            ]
            batch_results = await asyncio.gather(*tasks)

            for d, r in zip(batch, batch_results):
                r["ground_truth"] = d["ground_truth"]
                r["source_text"] = d["text"]
                results.append(r)

            success = sum(1 for r in batch_results if r["status"] == "success")
            print(f"[BATCH {batch_num}] Успешно: {success}/{len(batch)}")

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
    print(f"Обработано: {len(output_rows)}")
    print(f"Результат: {VERIFIED_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
