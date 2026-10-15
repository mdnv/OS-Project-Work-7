"""Сравнение SharedMemory и Pipe: 10^7 int32 (40 MB), контроль CRC32."""
import argparse
from array import array
import csv
import multiprocessing as mp
import os
from pathlib import Path
import statistics
import time
import uuid
import zlib
from multiprocessing import shared_memory

CHUNK_SIZE = 1_048_576  # 1 MiB: без ограничений на очень большие сообщения Pipe
BASE = Path(__file__).resolve().parent


def receive_shm(name, data_ready, started, result_sender):
    shm = shared_memory.SharedMemory(name=name)
    try:
        started.send(True)  # дочерний процесс готов: размер/подключение не замеряем
        data_ready.wait()  # явная синхронизация: не читать до завершения записи
        with shm.buf as data:
            checksum = zlib.crc32(data)
        result_sender.send(checksum)
    finally:
        started.close()
        result_sender.close()
        shm.close()


def receive_pipe(receiver, size, started, result_sender):
    try:
        started.send(True)
        remaining = size
        checksum = 0
        while remaining > 0:
            piece = receiver.recv_bytes()
            if not piece or len(piece) > remaining:
                raise RuntimeError("Неверный размер сообщения Pipe")
            checksum = zlib.crc32(piece, checksum)
            remaining -= len(piece)
        result_sender.send(checksum)
    finally:
        receiver.close()
        started.close()
        result_sender.close()


def run_shm(ctx, data, expected_crc):
    name = f"os_pr7_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    shm = shared_memory.SharedMemory(name=name, create=True, size=len(data))
    ready_rx, ready_tx = ctx.Pipe(duplex=False)
    result_rx, result_tx = ctx.Pipe(duplex=False)
    data_ready = ctx.Event()
    child = ctx.Process(target=receive_shm, args=(name, data_ready, ready_tx, result_tx))
    try:
        child.start()
        ready_tx.close()
        result_tx.close()
        if not ready_rx.poll(15) or not ready_rx.recv():
            raise RuntimeError("Процесс SharedMemory не готов")
        t0 = time.perf_counter()
        shm.buf[:] = data  # 40 MB копируется в общий сегмент
        data_ready.set()
        if not result_rx.poll(60):
            raise RuntimeError("SharedMemory: не получено подтверждение")
        checksum = result_rx.recv()
        seconds = time.perf_counter() - t0
        child.join(timeout=5)
        if child.exitcode != 0:
            raise RuntimeError(f"SharedMemory: процесс завершился с кодом {child.exitcode}")
        if checksum != expected_crc:
            raise RuntimeError("SharedMemory: CRC32 не совпал")
        return seconds
    finally:
        ready_rx.close()
        result_rx.close()
        if child.is_alive():
            child.terminate()
            child.join()
        shm.close()
        shm.unlink()  # /dev/shm/os_pr7_* удаляется даже при ошибке


def run_pipe(ctx, data, expected_crc):
    receiver, sender = ctx.Pipe(duplex=False)
    ready_rx, ready_tx = ctx.Pipe(duplex=False)
    result_rx, result_tx = ctx.Pipe(duplex=False)
    child = ctx.Process(target=receive_pipe, args=(receiver, len(data), ready_tx, result_tx))
    try:
        child.start()
        receiver.close()  # у родителя не остаётся читающего конца
        ready_tx.close()
        result_tx.close()
        if not ready_rx.poll(15) or not ready_rx.recv():
            raise RuntimeError("Процесс Pipe не готов")
        t0 = time.perf_counter()
        for offset in range(0, len(data), CHUNK_SIZE):
            sender.send_bytes(data[offset:offset + CHUNK_SIZE])
        sender.close()  # закрыть пишущий конец: получатель может увидеть EOF
        if not result_rx.poll(60):
            raise RuntimeError("Pipe: не получено подтверждение")
        checksum = result_rx.recv()
        seconds = time.perf_counter() - t0
        child.join(timeout=5)
        if child.exitcode != 0:
            raise RuntimeError(f"Pipe: процесс завершился с кодом {child.exitcode}")
        if checksum != expected_crc:
            raise RuntimeError("Pipe: CRC32 не совпал")
        return seconds
    finally:
        sender.close()
        receiver.close()
        ready_rx.close()
        result_rx.close()
        if child.is_alive():
            child.terminate()
            child.join()


def main():
    parser = argparse.ArgumentParser(description="Benchmark SharedMemory против Pipe")
    parser.add_argument("--count", type=int, default=10_000_000, help="Число int32 (по умолчанию 10^7)")
    parser.add_argument("--repeat", type=int, default=3, help="Число повторов (по умолчанию 3)")
    args = parser.parse_args()
    if not (1 <= args.count <= 10_000_000):
        parser.error("--count должен быть от 1 до 10000000")
    if not (1 <= args.repeat <= 20):
        parser.error("--repeat должен быть от 1 до 20")

    print(f"Создание массива из {args.count:,} чисел...", flush=True)
    values = array("i", range(args.count))
    if values.itemsize != 4:
        raise RuntimeError("В этой системе int не занимает 4 байта")
    ctx = mp.get_context("fork")  # запуск на Ubuntu Server 24.04
    with memoryview(values).cast("B") as data:
        expected_crc = zlib.crc32(data)
        print(f"Объём: {len(data):,} байт ({len(data)/1_000_000:.1f} MB); CRC32={expected_crc}", flush=True)
        results = {"SharedMemory": [], "Pipe": []}
        try:
            for number in range(1, args.repeat + 1):
                for mechanism, benchmark in (("SharedMemory", run_shm), ("Pipe", run_pipe)):
                    elapsed = benchmark(ctx, data, expected_crc)
                    results[mechanism].append(elapsed)
                    print(f"Повтор {number}/{args.repeat}: {mechanism:12} {elapsed:.4f} с | CRC32 OK", flush=True)
        except OSError as error:
            print(f"Ошибка ОС: {error}. Проверьте df -h /dev/shm и свободную RAM.", flush=True)
            raise

    destination = BASE / "benchmark_results.csv"
    with destination.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["mechanism", "synchronization", "elements", "bytes", "median_seconds"])
        for mechanism in ("SharedMemory", "Pipe"):
            median = statistics.median(results[mechanism])
            writer.writerow([mechanism, "Да (Event)" if mechanism == "SharedMemory" else "Нет (Pipe обеспечивает порядок)", args.count, args.count * 4, f"{median:.6f}"])
            print(f"МЕДИАНА {mechanism:12}: {median:.4f} с", flush=True)
    print(f"Таблица сохранена: {destination}")
    print("Проверка: ls /dev/shm | grep os_pr7_ (вывод должен быть пустым)")


if __name__ == "__main__":
    main()
