#!/usr/bin/env python3
"""
Brand Normalizer - Главный скрипт пайплайна нормализации брендов

Использование:
    python brand_normalizer.py           # Запуск полного пайплайна
    python brand_normalizer.py --step 1  # Только генерация синонимов
    python brand_normalizer.py --step 2  # Только матчинг
    python brand_normalizer.py --step 3  # Только LLM фильтрация
    python brand_normalizer.py --step 4  # Только генерация отчета
"""
import sys
import argparse
import subprocess
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent / "scripts"


def run_step(step_num: int, script_name: str, description: str) -> bool:
    """Запускает один шаг пайплайна"""
    print(f"\n{'='*70}")
    print(f"ШАГ {step_num}: {description}")
    print(f"{'='*70}")

    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        print(f"[ERROR] Скрипт не найден: {script_path}")
        return False

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=SCRIPTS_DIR
    )

    if result.returncode != 0:
        print(f"[ERROR] Шаг {step_num} завершился с ошибкой")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Brand Normalizer - Нормализация брендов в диалогах"
    )
    parser.add_argument(
        "--step", "-s",
        type=int,
        choices=[1, 2, 3, 4],
        help="Запустить только указанный шаг (1-4)"
    )
    parser.add_argument(
        "--skip-synonyms",
        action="store_true",
        help="Пропустить генерацию синонимов (использовать существующий файл)"
    )

    args = parser.parse_args()

    steps = [
        (1, "01_generate_synonyms.py", "Генерация синонимов брендов"),
        (2, "02_match_synonyms.py", "Сопоставление синонимов в диалогах"),
        (3, "03_filter_llm.py", "Верификация через LLM"),
        (4, "04_generate_report.py", "Генерация финального отчета"),
    ]

    print("="*70)
    print("BRAND NORMALIZER - Нормализация брендов в транскрибированных диалогах")
    print("="*70)

    if args.step:
        # Запуск одного шага
        step = next((s for s in steps if s[0] == args.step), None)
        if step:
            success = run_step(*step)
            sys.exit(0 if success else 1)
    else:
        # Полный пайплайн
        for step_num, script, desc in steps:
            if step_num == 1 and args.skip_synonyms:
                print(f"\n[SKIP] Шаг 1: Генерация синонимов (--skip-synonyms)")
                continue

            success = run_step(step_num, script, desc)
            if not success:
                print(f"\n[ABORT] Пайплайн остановлен на шаге {step_num}")
                sys.exit(1)

    print(f"\n{'='*70}")
    print("ПАЙПЛАЙН ЗАВЕРШЕН УСПЕШНО")
    print(f"{'='*70}")
    print("\nВыходные файлы находятся в директории: output/")
    print("  - result.csv")
    print("  - metrics.json")
    print("  - low_precision_brands.csv")
    print("  - report.xlsx")


if __name__ == "__main__":
    main()
