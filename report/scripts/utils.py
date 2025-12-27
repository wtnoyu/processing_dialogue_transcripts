"""
Общие утилиты для пайплайна нормализации брендов
"""
import os
import string
from pathlib import Path
from dotenv import load_dotenv

# Загрузка .env из корня проекта
ROOT_DIR = Path(__file__).parent.parent.parent
load_dotenv(ROOT_DIR / ".env")

# Пути
REPORT_DIR = Path(__file__).parent.parent
DATA_DIR = REPORT_DIR / "data"
OUTPUT_DIR = REPORT_DIR / "output"
SYNONYMS_DIR = REPORT_DIR / "synonyms"

# API настройки
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")
API_URL = "https://api.together.xyz/v1/chat/completions"
MODEL = "openai/gpt-oss-120b"

# Файлы данных
BRANDS_FILE = DATA_DIR / "brands.csv"
DIALOGS_FILE = DATA_DIR / "dialogs.csv"
SYNONYMS_FILE = SYNONYMS_DIR / "all_brand_synonyms.json"

# Выходные файлы
RESULT_FILE = OUTPUT_DIR / "result.csv"
METRICS_FILE = OUTPUT_DIR / "metrics.json"
LOW_PRECISION_FILE = OUTPUT_DIR / "low_precision_brands.csv"
REPORT_XLSX_FILE = OUTPUT_DIR / "report.xlsx"


def preprocess_text(text: str) -> str:
    """Удаление пунктуации и приведение к lowercase"""
    text = text.translate(str.maketrans('', '', string.punctuation))
    return text.lower()


def get_ngrams(words: list, max_n: int = 5) -> set:
    """Генерация всех n-грамм от 1 до max_n слов"""
    ngrams = set()
    for i in range(len(words)):
        for n in range(1, min(max_n + 1, len(words) - i + 1)):
            ngram = ' '.join(words[i:i+n])
            ngrams.add(ngram)
    return ngrams


def ensure_dirs():
    """Создание необходимых директорий"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    SYNONYMS_DIR.mkdir(exist_ok=True)
