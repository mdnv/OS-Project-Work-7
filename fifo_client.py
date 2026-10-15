"""Один FIFO-клиент. Не запускать одновременно с другим FIFO-клиентом."""
import argparse
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
REQUEST = BASE / "requests.fifo"
RESPONSE = BASE / "responses.fifo"


def main():
    parser = argparse.ArgumentParser(description="Отправить выражение FIFO-серверу")
    parser.add_argument("expression", help='Например: "(12 + 8) * 3"')
    args = parser.parse_args()
    if "\n" in args.expression:
        parser.error("Выражение должно занимать одну строку")
    if not REQUEST.is_fifo() or not RESPONSE.is_fifo():
        parser.error("Сначала запустите python3 fifo_server.py в другом SSH-терминале")

    start = time.perf_counter()
    # Порядок открытия MUST совпадать с сервером: request -> response.
    with REQUEST.open("w", encoding="utf-8") as writer:
        with RESPONSE.open("r", encoding="utf-8") as reader:
            writer.write(args.expression + "\n")
            writer.flush()
            result = reader.readline().strip()
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"Ответ сервера: {result}")
    print(f"Обмен: {elapsed_ms:.3f} мс | запрос: {len((args.expression + chr(10)).encode('utf-8'))} Б | ответ: {len((result + chr(10)).encode('utf-8'))} Б")


if __name__ == "__main__":
    main()
