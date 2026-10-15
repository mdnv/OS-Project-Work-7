"""FIFO-сервер: два именованных канала, клиенты ОБСЛУЖИВАЮТСЯ ПО ОЧЕРЕДИ."""
import os
import signal
from pathlib import Path
from calculator import calculate

BASE = Path(__file__).resolve().parent
REQUEST = BASE / "requests.fifo"
RESPONSE = BASE / "responses.fifo"


def stop_on_sigterm(signum, frame):
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, stop_on_sigterm)
    for path in (REQUEST, RESPONSE):
        if path.exists():
            if not path.is_fifo():
                raise RuntimeError(f"Не FIFO: {path}")
            path.unlink()  # удаление FIFO, оставшегося от прошлого прерванного запуска
        os.mkfifo(path, 0o600)

    print("FIFO-сервер запущен. Ожидаю клиентов по одному...", flush=True)
    print("Остановка: Ctrl+C", flush=True)
    try:
        while True:
            # open() именованных каналов может ждать второго участника — это нормально.
            with REQUEST.open("r", encoding="utf-8") as reader:
                with RESPONSE.open("w", encoding="utf-8") as writer:
                    expression = reader.readline().strip()
                    if not expression:
                        continue
                    result = calculate(expression)
                    writer.write(result + "\n")
                    writer.flush()
                    print(f"Запрос: {expression!r} -> {result}", flush=True)
    except KeyboardInterrupt:
        print("\nFIFO-сервер остановлен.", flush=True)
    finally:
        for path in (REQUEST, RESPONSE):
            if path.is_fifo():
                path.unlink()
        print("FIFO удалены.", flush=True)


if __name__ == "__main__":
    main()
