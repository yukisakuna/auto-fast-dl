import asyncio
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from main import DownloadManager


PAYLOAD = b"0123456789abcdef" * 16_384


class PayloadHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()
        self.wfile.write(PAYLOAD)

    def log_message(self, format, *args):
        return


class NoLengthHandler(PayloadHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Connection", "close")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(PAYLOAD)
        self.close_connection = True


class EmptyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()

    def log_message(self, format, *args):
        return


class ErrorHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_HEAD(self):
        self.send_response(500)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self.send_response(500)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):
        return


class LocalServer:
    def __init__(self, handler_class=PayloadHandler):
        self.handler_class = handler_class

    def __enter__(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.handler_class)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.url = f"http://{host}:{port}/payload.bin"
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class DownloadManagerTests(unittest.TestCase):
    def test_null_sink_downloads_all_repeats(self):
        with LocalServer() as server:
            manager = DownloadManager(
                concurrency=8,
                repeat=24,
                sink="null",
                progress=False,
                chunk_size=8192,
            )

            stats = asyncio.run(manager.run(server.url))

        self.assertEqual(stats.total_files, 24)
        self.assertEqual(stats.failed_downloads, 0)
        self.assertEqual(stats.total_bytes, len(PAYLOAD) * 24)

    def test_missing_content_length_does_not_crash(self):
        with LocalServer(NoLengthHandler) as server:
            manager = DownloadManager(
                concurrency=4,
                repeat=8,
                sink="null",
                progress=False,
                chunk_size=8192,
            )

            stats = asyncio.run(manager.run(server.url))

        self.assertEqual(stats.total_files, 8)
        self.assertEqual(stats.failed_downloads, 0)
        self.assertEqual(stats.total_bytes, len(PAYLOAD) * 8)

    def test_zero_byte_downloads_count_as_success(self):
        with LocalServer(EmptyHandler) as server:
            manager = DownloadManager(
                concurrency=4,
                repeat=6,
                sink="null",
                progress=False,
                chunk_size=1024,
            )

            stats = asyncio.run(manager.run(server.url))

        self.assertEqual(stats.total_files, 6)
        self.assertEqual(stats.failed_downloads, 0)
        self.assertEqual(stats.total_bytes, 0)

    def test_unbounded_disk_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            DownloadManager(
                concurrency=4,
                repeat=0,
                sink="disk",
                progress=False,
            )

    def test_max_failures_stops_retries(self):
        with LocalServer(ErrorHandler) as server:
            manager = DownloadManager(
                concurrency=1,
                repeat=10,
                sink="null",
                progress=False,
                max_failures=1,
            )

            stats = asyncio.run(manager.run(server.url))

        self.assertEqual(stats.total_files, 0)
        self.assertEqual(stats.failed_downloads, 1)
        self.assertIn("HTTP 500", stats.last_error)

    def test_disk_sink_writes_and_cleans_only_current_run_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            unrelated = output_dir / "keep-me.bin"
            unrelated.write_bytes(b"do not delete")

            with LocalServer() as server:
                manager = DownloadManager(
                    download_dir=output_dir,
                    concurrency=4,
                    repeat=5,
                    sink="disk",
                    progress=False,
                    chunk_size=4096,
                )

                stats = asyncio.run(manager.run(server.url))

            self.assertEqual(stats.total_files, 5)
            self.assertEqual(stats.failed_downloads, 0)
            self.assertEqual(stats.total_bytes, len(PAYLOAD) * 5)
            downloaded_files = sorted(output_dir.glob(f"{manager.run_id}-*.bin"))
            self.assertEqual(len(downloaded_files), 5)
            for downloaded_file in downloaded_files:
                self.assertEqual(downloaded_file.read_bytes(), PAYLOAD)

            manager.cleanup_files()
            self.assertEqual(unrelated.read_bytes(), b"do not delete")
            self.assertEqual(list(output_dir.glob(f"{manager.run_id}-*")), [])

    def test_invalid_output_directory_is_reported(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "not-a-directory"
            output_path.write_text("file")

            with LocalServer() as server:
                manager = DownloadManager(
                    download_dir=output_path,
                    concurrency=2,
                    repeat=1,
                    sink="disk",
                    progress=False,
                )

                with self.assertRaises(NotADirectoryError):
                    asyncio.run(manager.run(server.url))


if __name__ == "__main__":
    unittest.main()
