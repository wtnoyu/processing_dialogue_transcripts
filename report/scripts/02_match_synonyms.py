"""
Шаг 2: Сопоставление синонимов брендов в диалогах
Использует инвертированный индекс и n-граммы для быстрого поиска.
"""
import json
import pandas as pd
from collections import defaultdict
from pathlib import Path
from utils import (
    DIALOGS_FILE, SYNONYMS_FILE, OUTPUT_DIR,
    preprocess_text, get_ngrams, ensure_dirs
)

# Промежуточный файл
MATCHED_FILE = OUTPUT_DIR / "matched_candidates.csv"


def build_synonym_index(synonyms_data: list) -> tuple:
    """
    Строит инвертированный индекс синонимов.
    Возвращает (индекс, макс_слов, original_forms)
    """
    synonym_index = defaultdict(set)
    brand_original_form = {}
    max_words = 0

    for item in synonyms_data:
        if item.get("status") != "success":
            continue

        brand_name = item["original_brand"]
        if len(brand_name) <= 3:
            continue

        response = item.get("response", {})
        items = response.get("items", [])
        if not items:
            continue

        brand_data = items[0]

        # Original
        orig = brand_data.get("original", "").strip()
        if orig:
            processed = preprocess_text(orig)
            if processed:
                synonym_index[processed].add(brand_name)
                brand_original_form[brand_name] = processed
                max_words = max(max_words, len(processed.split()))

        # Variants (только > 3 символов)
        for variant_type in ["exact_variants", "phonetic_variants", "colloquial_variants"]:
            for var in brand_data.get(variant_type, []):
                var = var.strip()
                if var and len(var) > 3:
                    processed = preprocess_text(var)
                    if processed and len(processed) > 3:
                        synonym_index[processed].add(brand_name)
                        max_words = max(max_words, len(processed.split()))

    return synonym_index, max_words, brand_original_form


def match_brands_in_dialog(
    text: str,
    synonym_index: dict,
    max_words: int,
    brand_original_form: dict
) -> list:
    """
    Ищет бренды в тексте диалога.
    Возвращает список (brand_name, matched_synonym)
    """
    text_processed = preprocess_text(text)
    words = text_processed.split()
    ngrams = get_ngrams(words, max_n=max_words)

    found = {}  # brand -> matched_ngram

    for ngram in ngrams:
        if ngram in synonym_index:
            for brand in synonym_index[ngram]:
                if brand not in found:
                    found[brand] = ngram

    return [(brand, synonym) for brand, synonym in found.items()]


def main():
    """Главная функция матчинга"""
    ensure_dirs()

    print("=" * 60)
    print("ШАГ 2: СОПОСТАВЛЕНИЕ СИНОНИМОВ")
    print("=" * 60)

    # Загрузка синонимов
    if not SYNONYMS_FILE.exists():
        raise FileNotFoundError(f"Файл синонимов не найден: {SYNONYMS_FILE}")

    with open(SYNONYMS_FILE, 'r', encoding='utf-8') as f:
        synonyms_data = json.load(f)

    print(f"Загружено записей синонимов: {len(synonyms_data)}")

    # Построение индекса
    print("\nПостроение индекса...")
    synonym_index, max_words, brand_original_form = build_synonym_index(synonyms_data)
    print(f"  Уникальных синонимов: {len(synonym_index)}")
    print(f"  Макс. слов в синониме: {max_words}")

    # Загрузка диалогов
    if not DIALOGS_FILE.exists():
        raise FileNotFoundError(f"Файл диалогов не найден: {DIALOGS_FILE}")

    df = pd.read_csv(DIALOGS_FILE)
    print(f"\nДиалогов: {len(df)}")

    # Обработка
    print("\nПоиск брендов в диалогах...")
    results = []

    for idx, row in df.iterrows():
        if (idx + 1) % 50 == 0:
            print(f"  Прогресс: {idx + 1}/{len(df)}")

        dialog_id = row.get("source_text_index", idx)
        text = str(row.get("source_text", ""))

        # Ground truth
        try:
            gt_data = json.loads(row.get("extracted_brands", "[]"))
            ground_truth = [b.get("brand", "") for b in gt_data]
        except:
            ground_truth = []

        # Поиск
        matches = match_brands_in_dialog(text, synonym_index, max_words, brand_original_form)

        results.append({
            "dialog_id": dialog_id,
            "source_text": text,
            "ground_truth": json.dumps(ground_truth, ensure_ascii=False),
            "matched_brands": "\n".join([f"{b}|{s}" for b, s in matches]),
            "matched_count": len(matches)
        })

    # Сохранение
    results_df = pd.DataFrame(results)
    results_df.to_csv(MATCHED_FILE, index=False, encoding="utf-8-sig")

    # Статистика
    total_matches = sum(r["matched_count"] for r in results)
    dialogs_with_matches = sum(1 for r in results if r["matched_count"] > 0)

    print(f"\n{'='*60}")
    print("ГОТОВО")
    print(f"{'='*60}")
    print(f"Диалогов с найденными брендами: {dialogs_with_matches}/{len(df)}")
    print(f"Всего найденных кандидатов: {total_matches}")
    print(f"Результат: {MATCHED_FILE}")


if __name__ == "__main__":
    main()
