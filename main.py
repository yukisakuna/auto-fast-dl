from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid4

import aiohttp
from tqdm import tqdm


VERSION = "4.0.0"
DEFAULT_CONCURRENCY = min(128, max(32, (os.cpu_count() or 4) * 8))
MAX_CONCURRENCY = 512
DEFAULT_CHUNK_SIZE = 1024 * 1024
SinkMode = Literal["disk", "null"]


@dataclass
class DownloadStats:
    total_files: int = 0
    failed_downloads: int = 0
    total_bytes: int = 0
    start_time: float = field(default_factory=perf_counter)
    last_error: str | None = None

    @property
    def elapsed_seconds(self) -> float:
        return max(perf_counter() - self.start_time, 0.000001)

    @property
    def mib_per_second(self) -> float:
        return (self.total_bytes / (1024 * 1024)) / self.elapsed_seconds

    @property
    def files_per_second(self) -> float:
        return self.total_files / self.elapsed_seconds


class DownloadManager:
    def __init__(
        self,
        download_dir: str | Path = "downloads",
        *,
        concurrency: int = DEFAULT_CONCURRENCY,
        repeat: int = 1,
        sink: SinkMode = "disk",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout_seconds: float = 30.0,
        progress: bool = True,
        cleanup: bool = False,
        max_failures: int = 0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if concurrency > MAX_CONCURRENCY:
            raise ValueError(f"concurrency must be <= {MAX_CONCURRENCY}")
        if repeat < 0:
            raise ValueError("repeat must be >= 0; use 0 for infinite")
        if chunk_size < 1024:
            raise ValueError("chunk_size must be >= 1024 bytes")
        if repeat == 0 and sink == "disk":
            raise ValueError("repeat=0 is only supported with --sink null")

        self.download_dir = Path(download_dir)
        self.concurrency = concurrency
        self.repeat = repeat
        self.sink = sink
        self.chunk_size = chunk_size
        self.timeout_seconds = timeout_seconds
        self.progress = progress
        self.cleanup = cleanup
        self.max_failures = max_failures
        self.stats = DownloadStats()
        self.run_id = uuid4().hex[:12]
        self._next_job = 0
        self._stop = False
        self._stop_event: asyncio.Event | None = None

    async def run(self, url: str) -> DownloadStats:
        self.stats = DownloadStats()
        self.run_id = uuid4().hex[:12]
        self._next_job = 0
        self._stop = False
        self._validate_url(url)
        if self.sink == "disk":
            if self.download_dir.exists() and not self.download_dir.is_dir():
                raise NotADirectoryError(f"output directory is not a directory: {self.download_dir}")
            try:
                self.download_dir.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise OSError(f"failed to prepare output directory {self.download_dir}: {exc}") from exc

        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            limit_per_host=self.concurrency,
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=self.timeout_seconds,
            sock_read=self.timeout_seconds,
        )
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": f"auto-fast-dl/{VERSION}",
        }

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
            auto_decompress=False,
            trust_env=False,
        ) as session:
            self._stop_event = asyncio.Event()
            content_length = await self._probe_content_length(session, url)
            total_bytes = content_length * self.repeat if content_length and self.repeat else None
            self.stats.start_time = perf_counter()

            with tqdm(
                total=total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloaded",
                disable=not self.progress,
            ) as progress_bar:
                workers = [
                    asyncio.create_task(self._worker(session, url, progress_bar))
                    for _ in range(self.concurrency)
                ]
                stop_watcher = asyncio.create_task(self._stop_event.wait())
                try:
                    done, _ = await asyncio.wait(
                        {*workers, stop_watcher},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop_watcher in done:
                        for worker in workers:
                            worker.cancel()
                        await asyncio.gather(*workers, return_exceptions=True)
                    else:
                        await asyncio.gather(*workers)
                except BaseException:
                    for worker in workers:
                        worker.cancel()
                    stop_watcher.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                    await asyncio.gather(stop_watcher, return_exceptions=True)
                    raise
                finally:
                    stop_watcher.cancel()
                    await asyncio.gather(stop_watcher, return_exceptions=True)
                    if self.cleanup:
                        self.cleanup_files()

        return self.stats

    def cleanup_files(self) -> None:
        if not self.download_dir.exists():
            return

        for path in self.download_dir.glob(f"{self.run_id}-*"):
            if path.is_file():
                try:
                    path.unlink()
                except OSError as exc:
                    self.stats.last_error = f"cleanup failed for {path}: {exc}"

    async def _worker(
        self,
        session: aiohttp.ClientSession,
        url: str,
        progress_bar: tqdm,
    ) -> None:
        while True:
            job_id = self._reserve_job()
            if job_id is None:
                return
            await self._download_one(session, url, job_id, progress_bar)

    def _reserve_job(self) -> int | None:
        if self._stop:
            return None
        if self.repeat and self._next_job >= self.repeat:
            return None

        self._next_job += 1
        return self._next_job

    async def _download_one(
        self,
        session: aiohttp.ClientSession,
        url: str,
        job_id: int,
        progress_bar: tqdm,
    ) -> None:
        output_path = self._output_path(url, job_id)
        temp_path = output_path.with_suffix(output_path.suffix + ".part")

        try:
            async with session.get(url, allow_redirects=True) as response:
                if response.status < 200 or response.status >= 300:
                    self._record_failure(f"GET {url} returned HTTP {response.status}")
                    return

                if self.sink == "disk":
                    await self._stream_to_disk(response, temp_path, progress_bar)
                    temp_path.replace(output_path)
                else:
                    await self._stream_to_null(response, progress_bar)

                self.stats.total_files += 1
        except asyncio.CancelledError:
            if self.sink == "disk":
                temp_path.unlink(missing_ok=True)
            raise
        except Exception as exc:
            if self.sink == "disk":
                temp_path.unlink(missing_ok=True)
            self._record_failure(f"{type(exc).__name__}: {exc}")

    async def _stream_to_disk(
        self,
        response: aiohttp.ClientResponse,
        temp_path: Path,
        progress_bar: tqdm,
    ) -> int:
        bytes_written = 0
        with temp_path.open("wb", buffering=self.chunk_size) as file_obj:
            async for chunk in response.content.iter_chunked(self.chunk_size):
                if not chunk:
                    continue
                file_obj.write(chunk)
                bytes_written += len(chunk)
                self.stats.total_bytes += len(chunk)
                progress_bar.update(len(chunk))
        return bytes_written

    async def _stream_to_null(
        self,
        response: aiohttp.ClientResponse,
        progress_bar: tqdm,
    ) -> int:
        bytes_read = 0
        async for chunk in response.content.iter_chunked(self.chunk_size):
            if not chunk:
                continue
            bytes_read += len(chunk)
            self.stats.total_bytes += len(chunk)
            progress_bar.update(len(chunk))
        return bytes_read

    def _record_failure(self, message: str) -> None:
        self.stats.failed_downloads += 1
        self.stats.last_error = message
        if self.max_failures and self.stats.failed_downloads >= self.max_failures:
            self._stop = True
            if self._stop_event is not None:
                self._stop_event.set()

    def _output_path(self, url: str, job_id: int) -> Path:
        suffix = Path(urlsplit(url).path).suffix or ".bin"
        filename = f"{self.run_id}-{job_id:08d}{suffix}"
        return self.download_dir / filename

    async def _probe_content_length(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> int:
        try:
            async with session.head(url, allow_redirects=True) as response:
                if response.status < 200 or response.status >= 400:
                    return 0
                return int(response.headers.get("Content-Length") or 0)
        except Exception:
            return 0

    @staticmethod
    def _validate_url(url: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("URL must start with http:// or https://")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="High-throughput async downloader. Use only with URLs you own or are allowed to test.",
    )
    parser.add_argument("url", nargs="?", help="URL to download")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"parallel downloads, default: {DEFAULT_CONCURRENCY}, max: {MAX_CONCURRENCY}",
    )
    parser.add_argument(
        "-n",
        "--repeat",
        type=int,
        default=1,
        help="number of downloads; 0 means run until interrupted, default: 1",
    )
    parser.add_argument(
        "--sink",
        choices=("disk", "null"),
        default="disk",
        help="disk saves files; null discards bytes after receiving them, default: disk",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="downloads",
        help="directory for disk downloads, default: downloads",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"streaming chunk size in bytes, default: {DEFAULT_CHUNK_SIZE}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="socket connect/read timeout in seconds, default: 30",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="delete files created by this run after completion",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=0,
        help="stop after this many failed downloads; 0 disables the limit",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="disable tqdm progress output",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    return parser.parse_args()


def print_summary(stats: DownloadStats, interrupted: bool = False) -> None:
    status = "interrupted" if interrupted else "complete"
    print(
        f"{status}: files={stats.total_files} failed={stats.failed_downloads} "
        f"bytes={stats.total_bytes} elapsed={stats.elapsed_seconds:.2f}s "
        f"speed={stats.mib_per_second:.2f} MiB/s files/s={stats.files_per_second:.2f}"
    )
    if stats.last_error:
        print(f"last error: {stats.last_error}")


def main() -> int:
    args = parse_args()
    url = args.url or input("Enter the URL to download: ").strip()

    interrupted = False
    manager: DownloadManager | None = None
    try:
        manager = DownloadManager(
            download_dir=args.output_dir,
            concurrency=args.concurrency,
            repeat=args.repeat,
            sink=args.sink,
            chunk_size=args.chunk_size,
            timeout_seconds=args.timeout,
            progress=not args.no_progress,
            cleanup=args.cleanup,
            max_failures=args.max_failures,
        )
        stats = asyncio.run(manager.run(url))
    except KeyboardInterrupt:
        interrupted = True
        stats = manager.stats if manager is not None else DownloadStats()
    except (OSError, ValueError) as exc:
        print(f"error: {exc}")
        return 2

    print_summary(stats, interrupted=interrupted)
    return 1 if stats.failed_downloads else 0


if __name__ == "__main__":
    raise SystemExit(main())
