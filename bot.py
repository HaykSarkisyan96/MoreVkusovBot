import asyncio
import json
import math
from pathlib import Path

import pandas as pd
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command


# ==== НАСТРОЙКИ ====
BOT_TOKEN = "8316161069:AAH589YlLxr9Y0Hv36nIBItJVNbo9jrHLHU"
SHOP_NAME = "ЮЖНАЯ ЛАВКА"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

SUPPLIERS = {}  # конфиг-поставщиков (загружается из suppliers.json)


# ==== ЗАГРУЗКА ФАЙЛА suppliers.json ====

def load_suppliers():
    global SUPPLIERS
    with open("suppliers.json", "r", encoding="utf-8") as f:
        SUPPLIERS = json.load(f)


load_suppliers()


# ==== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====

def detect_supplier_name(df: pd.DataFrame) -> str | None:
    """
    Определяем название поставщика:
    - если есть "Товарная подгруппа" → берём её
    - иначе → "Товарная группа"
    """
    for col in ["Товарная подгруппа", "Товарная группа"]:
        if col in df.columns:
            series = df[col].dropna()
            if not series.empty:
                return str(series.iloc[0]).strip()
    return None


def detect_supplier_config(supplier_name: str):
    """
    Ищем конфиг в suppliers.json.
    Если нет — по умолчанию считаем в штуках.
    """
    name = supplier_name.lower().strip()

    for key in SUPPLIERS.keys():
        if key.lower() in name:
            return SUPPLIERS[key]

    return {"type": "pieces"}  # дефолт


def parse_volume_from_name(name: str) -> str | None:
    """
    Ищем объём в названии товара.
    Возвращаем строку '0.5', '1', '1.5' и т.п.
    """
    if not isinstance(name, str):
        return None

    text = name.lower().replace(",", ".")
    tokens = text.split()

    possible = []
    for token in tokens:
        token = token.replace("л", "").replace("l", "")
        token = token.strip()

        try:
            float(token)
            possible.append(token)
        except:
            continue

    if not possible:
        return None

    return possible[0]


def build_order_text(df: pd.DataFrame, supplier_name: str) -> str:
    """
    Формируем текст заявки по конфигу поставщика.
    """

    supplier_cfg = detect_supplier_config(supplier_name)
    supplier_type = supplier_cfg["type"]

    # Проверка числовых колонок
    for col in ["Расход", "Конечный остаток"]:
        if col not in df.columns:
            raise ValueError(f"Нет колонки '{col}'")
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Базовая формула
    df["base_qty"] = df["Расход"] - df["Конечный остаток"]
    df = df[df["base_qty"] > 0]  # только положительные заявки

    if df.empty:
        return f"{supplier_name}: {SHOP_NAME}\n(Нет позиций для заказа)"

    lines = [f"{supplier_name}: {SHOP_NAME}"]

    # ==========================
    # TYPE 1: PIECES (шт)
    # ==========================
    if supplier_type == "pieces":
        df["order_qty"] = df["base_qty"].round().astype(int)
        unit = "шт"

    # ==========================
    # TYPE 2: VOLUME_PACK (упаковки по объёму)
    # ==========================
    elif supplier_type == "volume_pack":
        unit = "уп"
        volume_map = supplier_cfg.get("volumes", {})

        def get_pack_size(name):
            volume = parse_volume_from_name(name)
            if not volume:
                return 1
            return volume_map.get(volume, 1)

        df["pack_size"] = df["Название"].apply(get_pack_size)

        df["order_qty"] = (df["base_qty"] / df["pack_size"]).apply(
            lambda x: int(math.ceil(x)) if x > 0 else 0
        )

        df = df[df["order_qty"] > 0]

    else:
        # fallback
        df["order_qty"] = df["base_qty"].round().astype(int)
        unit = "шт"

    if df.empty:
        return f"{supplier_name}: {SHOP_NAME}\n(Нет позиций для заказа)"

    # Формируем текст заявки
    counter = 1
    for _, row in df.iterrows():
        name = str(row["Название"]).strip()
        qty = int(row["order_qty"])

        if qty <= 0:
            continue

        lines.append(f"{counter}. {name} - {qty}{unit}")
        counter += 1

    return "\n".join(lines)


def is_excel(filename: str) -> bool:
    ext = filename.split(".")[-1].lower()
    return ext in ("xls", "xlsx")


# ==== TELEGRAM HANDLERS ====

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        "Привет! Я бот для формирования заявок 📦\n\n"
        "Отправь мне Excel-файл отчёта, и я автоматически сформирую заявку.\n"
        "Логика расчёта зависит от поставщика (шт / упаковки / объём),\n"
        "данные берутся из suppliers.json."
    )


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "Как пользоваться:\n"
        "1️⃣ Отправь Excel-файл (.xls или .xlsx)\n"
        "2️⃣ Бот определит поставщика\n"
        "3️⃣ Посчитает заявку по правилам из suppliers.json\n"
        "4️⃣ Вернёт текст заявки\n"
    )


@dp.message(F.document)
async def handle_document(message: types.Message):
    doc: types.Document = message.document

    if not is_excel(doc.file_name):
        await message.answer("Мне нужен Excel-файл (.xls или .xlsx) 📄")
        return

    await message.answer("Файл получил, обрабатываю... ⏳")

    # директория
    files_dir = Path("files")
    files_dir.mkdir(exist_ok=True)

    file_path = files_dir / f"{doc.file_unique_id}_{doc.file_name}"

    try:
        file_info = await bot.get_file(doc.file_id)
        await bot.download_file(file_info.file_path, destination=file_path)

        df = pd.read_excel(file_path)

        supplier_name = detect_supplier_name(df)
        if not supplier_name:
            await message.answer("Не смог определить поставщика 😕")
            return

        order_text = build_order_text(df, supplier_name)

        await message.answer(f"Готовая заявка:\n\n{order_text}")

    except Exception as e:
        await message.answer(f"Ошибка при обработке файла:\n{e}")

    finally:
        if file_path.exists():
            file_path.unlink()


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
