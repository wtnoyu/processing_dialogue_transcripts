"""
Шаг 4: Генерация финального отчета
Формирует result.csv, metrics.json, low_precision_brands.csv, report.xlsx
"""
import json
import pandas as pd
from collections import defaultdict
from pathlib import Path
from utils import (
    OUTPUT_DIR, RESULT_FILE, METRICS_FILE,
    LOW_PRECISION_FILE, REPORT_XLSX_FILE, ensure_dirs
)

# Входной файл (результат шага 3)
VERIFIED_FILE = OUTPUT_DIR / "verified_brands.csv"


def normalize_brand(brand: str) -> str:
    """Нормализация названия бренда для сравнения"""
    b = ' '.join(brand.lower().strip().split())
    mappings = {
        'все инструменты, точка ру': 'всеинструменты.ру',
        'все инструменты': 'всеинструменты.ру',
        'всеинструменты': 'всеинструменты.ру',
        'сбер': 'sber',
        'сбербанк': 'sber',
        'озон': 'ozon',
        'яндекс': 'yandex',
        'сдек': 'сдэк',
        'cdek': 'сдэк',
    }
    return mappings.get(b, b)


def calculate_metrics(df: pd.DataFrame) -> tuple:
    """
    Рассчитывает Precision, Recall, F1.
    Возвращает (metrics_dict, brand_stats, result_rows)
    """
    brand_stats = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})
    result_rows = []

    total_tp = 0
    total_fp = 0
    total_fn = 0

    for _, row in df.iterrows():
        dialog_id = row["dialog_id"]
        source_text = row["source_text"]

        # Ground truth
        try:
            gt_data = json.loads(row["ground_truth"])
            gt_brands = {normalize_brand(b) for b in gt_data if b}
        except:
            gt_brands = set()

        # Predicted
        try:
            verified = json.loads(row["verified_brands"])
            predicted = {}  # brand -> {quotes, confidence}
            for item in verified:
                name = normalize_brand(item["name"])
                predicted[name] = {
                    "quotes": item.get("quotes", []),
                    "confidence": item.get("confidence", 0)
                }
        except:
            predicted = {}

        predicted_brands = set(predicted.keys())

        # TP, FP, FN
        tp = gt_brands & predicted_brands
        fp = predicted_brands - gt_brands
        fn = gt_brands - predicted_brands

        total_tp += len(tp)
        total_fp += len(fp)
        total_fn += len(fn)

        # Per-brand stats
        for brand in tp:
            brand_stats[brand]["tp"] += 1
        for brand in fp:
            brand_stats[brand]["fp"] += 1
        for brand in fn:
            brand_stats[brand]["fn"] += 1

        # Result rows (для result.csv)
        for brand_name, info in predicted.items():
            for quote in info["quotes"]:
                result_rows.append({
                    "dialog_id": dialog_id,
                    "original_text": quote,
                    "detected_brand": quote,  # Текст из цитаты
                    "normalized_brand": brand_name,
                    "confidence": info["confidence"]
                })

    # Aggregate metrics
    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    metrics = {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "true_positives": total_tp,
        "false_positives": total_fp,
        "false_negatives": total_fn
    }

    return metrics, brand_stats, result_rows


def generate_low_precision_brands(brand_stats: dict, top_n: int = 10) -> pd.DataFrame:
    """Генерирует список брендов с худшим Precision"""
    rows = []
    for brand, stats in brand_stats.items():
        tp = stats["tp"]
        fp = stats["fp"]
        fn = stats["fn"]
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0

        rows.append({
            "brand": brand,
            "precision": round(precision * 100, 2),
            "tp": tp,
            "fp": fp,
            "fn": fn
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(["precision", "fp"], ascending=[True, False])
    return df.head(top_n)


def generate_report_xlsx(df: pd.DataFrame, metrics: dict) -> None:
    """Генерирует Excel отчет в формате report_streamlined_workflow"""
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        print("openpyxl не установлен, пропускаем генерацию XLSX")
        return

    # Подготовка данных
    report_rows = []
    for _, row in df.iterrows():
        try:
            gt = json.loads(row["ground_truth"])
            verified = json.loads(row["verified_brands"])

            gt_brands = {normalize_brand(b) for b in gt if b}
            pred_brands = {normalize_brand(v["name"]) for v in verified}

            tp = gt_brands & pred_brands
            fp = pred_brands - gt_brands
            fn = gt_brands - pred_brands

            report_rows.append({
                "dialog_id": row["dialog_id"],
                "ground_truth": ", ".join(gt),
                "predicted": ", ".join([v["name"] for v in verified]),
                "true_positives": ", ".join(tp) if tp else "",
                "false_positives": ", ".join(fp) if fp else "",
                "false_negatives": ", ".join(fn) if fn else "",
                "status": "OK" if not fp and not fn else "DIFF"
            })
        except:
            continue

    report_df = pd.DataFrame(report_rows)

    # Запись в Excel
    with pd.ExcelWriter(REPORT_XLSX_FILE, engine='openpyxl') as writer:
        # Лист 1: Метрики
        metrics_df = pd.DataFrame([metrics])
        metrics_df.to_excel(writer, sheet_name='Metrics', index=False)

        # Лист 2: Детали
        report_df.to_excel(writer, sheet_name='Details', index=False)

    print(f"Excel отчет: {REPORT_XLSX_FILE}")


def main():
    """Главная функция генерации отчета"""
    ensure_dirs()

    print("=" * 60)
    print("ШАГ 4: ГЕНЕРАЦИЯ ОТЧЕТА")
    print("=" * 60)

    # Загрузка данных
    if not VERIFIED_FILE.exists():
        raise FileNotFoundError(f"Файл не найден: {VERIFIED_FILE}")

    df = pd.read_csv(VERIFIED_FILE)
    print(f"Загружено записей: {len(df)}")

    # Расчет метрик
    print("\nРасчет метрик...")
    metrics, brand_stats, result_rows = calculate_metrics(df)

    # 1. result.csv
    result_df = pd.DataFrame(result_rows)
    result_df.to_csv(RESULT_FILE, index=False, encoding="utf-8-sig")
    print(f"result.csv: {len(result_df)} записей")

    # 2. metrics.json
    with open(METRICS_FILE, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # 3. low_precision_brands.csv
    low_precision_df = generate_low_precision_brands(brand_stats)
    low_precision_df.to_csv(LOW_PRECISION_FILE, index=False, encoding="utf-8-sig")

    # 4. report.xlsx
    generate_report_xlsx(df, metrics)

    # Вывод результатов
    print(f"\n{'='*60}")
    print("РЕЗУЛЬТАТЫ")
    print(f"{'='*60}")
    print(f"Precision: {metrics['precision']:.4f} ({metrics['precision']*100:.2f}%)")
    print(f"Recall:    {metrics['recall']:.4f} ({metrics['recall']*100:.2f}%)")
    print(f"F1:        {metrics['f1']:.4f} ({metrics['f1']*100:.2f}%)")
    print(f"\nTP: {metrics['true_positives']}, FP: {metrics['false_positives']}, FN: {metrics['false_negatives']}")

    if metrics['f1'] > 0.4:
        print(f"\n[OK] F1 = {metrics['f1']:.4f} > 0.4 - ЦЕЛЬ ДОСТИГНУТА!")
    else:
        print(f"\n[!!] F1 = {metrics['f1']:.4f} <= 0.4 - цель не достигнута")

    print(f"\n{'='*60}")
    print("ВЫХОДНЫЕ ФАЙЛЫ")
    print(f"{'='*60}")
    print(f"  {RESULT_FILE}")
    print(f"  {METRICS_FILE}")
    print(f"  {LOW_PRECISION_FILE}")
    print(f"  {REPORT_XLSX_FILE}")


if __name__ == "__main__":
    main()
