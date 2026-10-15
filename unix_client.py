"""Клиент Unix-сокета. --demo запускает несколько клиентов-процессов одновременно."""
import argparse
import multiprocessing as mp
import os
import socket
import time
from pathlib import Path

SOCKET_PATH = Path(__file__).resolve().parent / "calculator.sock"
EXAMPLES = ["2+2", "10*7", "(12+8)*3", "100/4", "2**8", "21%5"]


def send_expression(expression):
    start = time.perf_counter()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(SOCKET_PATH))
            with client.makefile("rwb") as channel:
                channel.write((expression + "\n").encode("utf-8"))
                channel.flush()
                result = channel.readline().decode("utf-8").strip()
    except (OSError, UnicodeError) as error:
        print(f"Ошибка подключения/обмена: {error}. Сначала запустите unix_server.py.", flush=True)
        return False
    ms = (time.perf_counter() - start) * 1000
    sent = len((expression + "\n").encode("utf-8"))
    received = len((result + "\n").encode("utf-8"))
    print(f"PID={os.getpid()} | {expression} = {result} | {ms:.3f} мс | запрос: {sent} Б | ответ: {received} Б", flush=True)
    return True


def demo_worker(expression, start_event):
    start_event.wait()
    if not send_expression(expression):
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description="Клиент для Unix-сокета")
    parser.add_argument("expression", nargs="?", default="2+2")
    parser.add_argument("--demo", action="store_true", help="6 одновременных клиентов-процессов")
    args = parser.parse_args()
    if not SOCKET_PATH.is_socket():
        parser.error("Сначала запустите python3 unix_server.py в другом SSH-терминале")
    if args.demo:
        ctx = mp.get_context("fork")
        go = ctx.Event()
        clients = [ctx.Process(target=demo_worker, args=(expression, go)) for expression in EXAMPLES]
        for client in clients:
            client.start()
        print("Запущено 6 клиентских процессов. Отправляем запросы одновременно...", flush=True)
        go.set()
        for client in clients:
            client.join()
        if any(client.exitcode != 0 for client in clients):
            raise SystemExit("Некоторые клиенты завершились с ошибкой")
        print("Все 6 клиентов получили ответы.")
    else:
        if "\n" in args.expression:
            parser.error("Выражение должно занимать одну строку")
        if not send_expression(args.expression):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
