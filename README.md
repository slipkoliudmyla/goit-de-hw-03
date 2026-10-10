# goit-de-hw-03 — Аналіз даних у PySpark

Домашнє завдання до теми «Apache Spark. Основи і синтаксис».

## Структура

| Файл | Що всередині |
|---|---|
| `main.py` | увесь код ДЗ (кроки 1–6) |
| `screenshots/p1_…p6_*.png` | скриншоти `df.show()` для кожного кроку |
| `colab_run.ipynb` | ноутбук, у якому код запускався (Google Colab) |

## Дані

Три CSV-файли з умови ДЗ треба покласти поруч із `main.py`:
`users.csv`, `purchases.csv`, `products.csv`.

## Запуск

```bash
pip install pyspark
python main.py
```

Код запускався на PySpark 3.5.3 (Google Colab, local mode).

## Що робить кожен крок

1. **Читання** — кожен CSV читається в окремий DataFrame (`header=True, inferSchema=True`).
2. **Очищення** — `dropna()` видаляє рядки з пропусками: users 100 → 95, purchases 200 → 195, products 50 → 47.
3. **Сума покупок за категоріями** — `purchases ⋈ products` по `product_id`, сума покупки = `quantity * price`, потім `groupBy("category")`.
4. **Сума покупок для віку 18–25** — додатково `⋈ users` по `user_id` і фільтр `18 <= age <= 25`.
5. **Частка кожної категорії** — сума категорії / загальні витрати 18–25 × 100, округлення до 2 знаків.
6. **Топ-3 категорії** — сортування за відсотком і `limit(3)`.

## Результат (вік 18–25)

| category | percentage |
|---|---|
| Home | 29.9 |
| Sports | 25.71 |
| Electronics | 20.67 |
