"""Unix-сокет: отдельный дочерний ПРОЦЕСС для каждого подключения."""
import multiprocessing as mp
import signal
import socket
from pathlib import Path

from calculator import calculate

SOCKET_PATH = Path(__file__).resolve().parent / "calculator.sock"


def stop_on_sigterm(signum, frame):
    raise KeyboardInterrupt


def handle_client(connection):
    # Ctrl+C предназначен для сервера; работники игнорируют SIGINT.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    with connection:
        try:
            with connection.makefile("rwb") as channel:
                line = channel.readline(4096)
                expression = line.decode("utf-8").strip()
                result = calculate(expression)
                channel.write((result + "\n").encode("utf-8"))
                channel.flush()
                print(f"Дочерний PID={mp.current_process().pid}: {expression!r} -> {result}", flush=True)
        except (OSError, UnicodeError) as error:
            print(f"Клиент отключился / ошибка: {error}", flush=True)


def main():
    signal.signal(signal.SIGTERM, stop_on_sigterm)
    # В Ubuntu 24.04 Python 3.12 доступен fork; по процессу на клиента.
    ctx = mp.get_context("fork")
    children = []
    if SOCKET_PATH.exists():
        if not SOCKET_PATH.is_socket():
            raise RuntimeError(f"Не сокет: {SOCKET_PATH}")
        SOCKET_PATH.unlink()  # оставшийся сокет после аварийного завершения

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(str(SOCKET_PATH))
        server.listen(20)
        print(f"Unix-сервер: {SOCKET_PATH}", flush=True)
        print("Остановка: Ctrl+C", flush=True)
        while True:
            connection, _ = server.accept()
            worker = ctx.Process(target=handle_client, args=(connection,))
            try:
                worker.start()
                children.append(worker)
                print(f"Создан процесс PID={worker.pid}", flush=True)
            finally:
                connection.close()  # родитель закрывает свой экземпляр соединения
            for process in children[:]:
                if not process.is_alive():
                    process.join()  # убрать завершённые дочерние процессы (зомби)
                    children.remove(process)
    except KeyboardInterrupt:
        print("\nUnix-сервер остановлен.", flush=True)
    finally:
        server.close()
        for process in children:
            process.join(timeout=2)
            if process.is_alive():
                process.terminate()
                process.join()
        if SOCKET_PATH.is_socket():
            SOCKET_PATH.unlink()
        print("Сокет удалён.", flush=True)


if __name__ == "__main__":
    main()
