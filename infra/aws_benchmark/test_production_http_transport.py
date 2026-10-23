"""Real loopback TLS/socket regressions; no external connections or credentials."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import ssl
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from . import production_auth as auth, production_github_app as app
from .production_github import GitHubRateLimitError
from .test_production_github_app import config, PATH


class ClosingTlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "offline test authority")])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(app.API_HOST),
                x509.DNSName("token.actions.githubusercontent.com")]), critical=False)
            .sign(key, hashes.SHA256()))
        cls.cert = Path(cls.directory.name) / "test-ca.pem"
        cls.cert.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        pem = Path(cls.directory.name) / "test-key.pem"
        pem.write_bytes(key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        cls.server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cls.server_context.load_cert_chain(cls.cert, pem)

    @contextmanager
    def reply(self, payload, *, host, protocol="HTTP/1.1", status=200, headers="", trusted=True):
        listener = socket.socket()
        listener.settimeout(5)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        address, failures, requests, transports = listener.getsockname(), [], [], []
        wire = (f"{protocol} {status} test\r\nContent-Length: {len(payload)}\r\n"
            f"Connection: close\r\n{headers}\r\n").encode() + payload
        def serve():
            try:
                raw, _ = listener.accept()
                with raw:
                    raw.settimeout(5)
                    with self.server_context.wrap_socket(raw, server_side=True) as secure:
                        request = b""
                        while b"\r\n\r\n" not in request:
                            chunk = secure.recv(4096)
                            if not chunk or len(request) + len(chunk) > 16384:
                                raise AssertionError("invalid offline request")
                            request += chunk
                        requests.append(request)
                        secure.sendall(wire)
            except ssl.SSLError as error:
                if trusted:
                    failures.append(error)
            except Exception as error:
                failures.append(error)
        original_connect = socket.create_connection
        original_wrap = ssl.SSLContext.wrap_socket
        context = ssl.create_default_context(cafile=str(self.cert) if trusted else None)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        def connect(target, timeout, source_address=None):
            self.assertEqual(target, (host, 443))
            return original_connect(address, timeout, source_address)
        def wrap(current, raw, *args, **kwargs):
            secure = original_wrap(current, raw, *args, **kwargs)
            if not kwargs.get("server_side"):
                self.assertEqual(kwargs.get("server_hostname"), host)
                self.assertTrue(current.check_hostname)
                self.assertEqual(current.verify_mode, ssl.CERT_REQUIRED)
                transports.append(secure)
            return secure
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        try:
            with patch("socket.create_connection", side_effect=connect), \
                    patch.object(ssl, "create_default_context", return_value=context), \
                    patch.object(ssl.SSLContext, "wrap_socket", wrap):
                yield requests, transports
        finally:
            listener.close()
            worker.join(timeout=6)
            self.assertFalse(worker.is_alive(), "offline TLS fixture did not finish")
            if failures:
                raise failures[0]
            self.assertTrue(all(secure.fileno() == -1 for secure in transports))

    def test_app_body_survives_real_tls_content_length_socket_close(self):
        payload = b'{"value":"' + b"x" * 40000 + b'"}'
        for protocol in ("HTTP/1.1", "HTTP/1.0"):
            with self.subTest(protocol=protocol), self.reply(payload, host=app.API_HOST, protocol=protocol) as (requests, sockets):
                result = app._https("GET", PATH, {"Accept": "application/json"}, None,
                    timeout=3, deadline=time.monotonic() + 3, monotonic=time.monotonic)
                self.assertEqual(result.body, payload)
                self.assertEqual(result.status, 200)
                self.assertTrue(requests[0].startswith(("GET " + PATH + " HTTP/1.1\r\n").encode()))
                self.assertEqual(len(sockets), 1)
                self.assertEqual(sockets[0].fileno(), -1)

    def test_jwks_body_survives_real_tls_content_length_socket_close(self):
        payload = b'{"keys":[]}'
        for protocol in ("HTTP/1.1", "HTTP/1.0"):
            with self.subTest(protocol=protocol), self.reply(payload, host="token.actions.githubusercontent.com", protocol=protocol) as (requests, sockets):
                self.assertEqual(auth._fetch_official_jwks(), payload)
                self.assertTrue(requests[0].startswith(b"GET /.well-known/jwks HTTP/1.1\r\n"))
                self.assertEqual(len(sockets), 1)
                self.assertEqual(sockets[0].fileno(), -1)

    def test_closing_quota_response_keeps_retry_classification(self):
        provider = app.GitHubAppProvider(config())
        with self.reply(b'{"message":"rate limited"}', host=app.API_HOST, status=429, headers="Retry-After: 120\r\n"):
            with provider.observation() as observation, self.assertRaises(GitHubRateLimitError) as caught:
                provider._request(observation, "GET", PATH, "synthetic-offline-token")
            self.assertEqual(caught.exception.retry_after_seconds, 120)

    def test_untrusted_tls_certificate_is_still_rejected(self):
        for kind, host in (("app", app.API_HOST), ("jwks", "token.actions.githubusercontent.com")):
            with self.subTest(kind=kind), self.reply(b"{}", host=host, trusted=False):
                with self.assertRaises(ssl.SSLCertVerificationError):
                    if kind == "app":
                        app._https("GET", PATH, {}, None, timeout=3,
                            deadline=time.monotonic() + 3, monotonic=time.monotonic)
                    else:
                        auth._fetch_official_jwks()
