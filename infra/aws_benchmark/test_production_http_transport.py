"""Real loopback TLS/socket regressions; no external connections or credentials."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import os
import ssl
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from . import production_auth as auth, production_github_app as app, production_client as client
from .production_github import GitHubRunError, GitHubRateLimitError, GitHubTransientError
from .production_transport import retryable_transport
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
    def reply(self, payload, *, host, protocol="HTTP/1.1", status=200, headers="", trusted=True, declared_length=None, hostname_in_context=True):
        listener = socket.socket()
        listener.settimeout(5)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        address, failures, requests, transports = listener.getsockname(), [], [], []
        length = len(payload) if declared_length is None else declared_length
        wire = (f"{protocol} {status} test\r\nContent-Length: {length}\r\n"
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
                            if not chunk and not trusted:
                                return
                            if not chunk or len(request) + len(chunk) > 16384:
                                raise AssertionError("invalid offline request")
                            request += chunk
                        requests.append(request)
                        secure.sendall(wire)
            except (ssl.SSLError, BrokenPipeError, ConnectionResetError) as error:
                # rejection can abort before HTTP bytes reach this local server
                if trusted:
                    failures.append(error)
            except Exception as error:
                failures.append(error)
        original_connect = socket.create_connection
        original_wrap = ssl.SSLContext.wrap_socket
        context = ssl.create_default_context(cafile=str(self.cert) if trusted else None)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        def connect(target, timeout, source_address=None, **kwargs):
            self.assertEqual(target, (host, 443))
            return original_connect(address, timeout, source_address)
        def wrap(current, raw, *args, **kwargs):
            secure = original_wrap(current, raw, *args, **kwargs)
            if not kwargs.get("server_side"):
                self.assertEqual(kwargs.get("server_hostname"), host)
                if hostname_in_context:
                    self.assertTrue(current.check_hostname)
                self.assertEqual(current.verify_mode, ssl.CERT_REQUIRED)
                transports.append(secure)
            return secure
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        try:
            with patch("socket.create_connection", side_effect=connect), \
                    patch("urllib3.util.connection.create_connection", side_effect=connect), \
                    patch.object(ssl, "create_default_context", return_value=context), \
                    patch.object(ssl, "_create_default_https_context", return_value=context), \
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

    def test_truncated_content_length_is_transient_before_json_parse(self):
        cases = (("app", app.API_HOST, GitHubTransientError),
                 ("jwks", "token.actions.githubusercontent.com", auth.GitHubIdentityTransientError),
                 ("token", app.API_HOST, client.AdmissionRetryable))
        for kind, host, error in cases:
            for payload in (b'{"value":"synthetic-token"}', b'{"value":"cut'):
                with self.subTest(kind=kind, payload=payload), self.reply(payload, host=host,
                        declared_length=len(payload) + 20), self.assertRaises(error):
                    if kind == "app":
                        app._https("GET", PATH, {}, None, timeout=3,
                            deadline=time.monotonic() + 3, monotonic=time.monotonic)
                    elif kind == "jwks":
                        auth._fetch_official_jwks()
                    else:
                        with patch.dict(os.environ, dict(ACTIONS_ID_TOKEN_REQUEST_URL="https://" + host + "/oidc",
                                ACTIONS_ID_TOKEN_REQUEST_TOKEN="synthetic-token")):
                            client.token("test-audience")

    def test_complete_token_reply_remains_accepted(self):
        with self.reply(b'{"value":"synthetic-token"}', host=app.API_HOST), patch.dict(os.environ,
                dict(ACTIONS_ID_TOKEN_REQUEST_URL="https://" + app.API_HOST + "/oidc",
                     ACTIONS_ID_TOKEN_REQUEST_TOKEN="synthetic-request-token")):
            self.assertEqual(client.token("test-audience"), "synthetic-token")

    def test_sdk_certificate_failures_are_permanent_for_client_and_secret_reader(self):
        from botocore.awsrequest import AWSPreparedRequest
        from botocore.exceptions import SSLError as SdkSSLError
        from botocore.httpsession import URLLib3Session
        for host, verify in ((app.API_HOST, True), ("mismatch.example", str(self.cert))):
            with self.subTest(host=host), self.reply(b"{}", host=host, trusted=False, hostname_in_context=False):
                transport = URLLib3Session(verify=verify, proxies={}, timeout=3)
                try:
                    with self.assertRaises(SdkSSLError) as caught:
                        transport.send(AWSPreparedRequest("GET", "https://" + host + "/", {}, None, False))
                finally:
                    transport.close()
            error = caught.exception
            self.assertFalse(retryable_transport(error))
            operation, sleep = Mock(side_effect=error), Mock()
            with self.assertRaises(SdkSSLError):
                client.reconcile(operation, deadline=1000, reserve=10, clock=lambda: 1, sleep=sleep)
            self.assertEqual(operation.call_count, 1)
            sleep.assert_not_called()
            secrets = Mock()
            secrets.get_secret_value.side_effect = error
            provider = app.GitHubAppProvider(config(), secrets=secrets)
            with provider.observation() as observation, self.assertRaises(GitHubRunError) as sanitized:
                observation(PATH)
            self.assertNotIsInstance(sanitized.exception, GitHubTransientError)
            self.assertNotIn(host, str(sanitized.exception))
            self.assertEqual(secrets.get_secret_value.call_count, 1)

    def test_wrapped_tls_outages_remain_retryable_without_string_matching(self):
        from botocore.exceptions import SSLError as SdkSSLError
        from urllib3.exceptions import SSLError as PoolSSLError
        for inner, retry in ((ssl.SSLEOFError("interrupted"), True),
                             (ssl.SSLCertVerificationError("certificate"), False)):
            wrapped = SdkSSLError(endpoint_url="https://synthetic.invalid", error=PoolSSLError(inner))
            self.assertEqual(retryable_transport(wrapped), retry)
        self.assertFalse(retryable_transport(client.http.client.InvalidURL("bad URL")))
        self.assertFalse(retryable_transport(client.urllib.error.URLError(ssl.SSLCertVerificationError("bad"))))

    def test_truncated_oidc_fetch_retries_before_sending_seal(self):
        reads, pauses, invoke = [], [], Mock(return_value="sealed")
        def operation():
            reads.append("token")
            payload = b'{"value":"synthetic-token"}'
            length = len(payload) + (1 if len(reads) == 1 else 0)
            with self.reply(payload, host=app.API_HOST, declared_length=length), patch.dict(os.environ,
                    dict(ACTIONS_ID_TOKEN_REQUEST_URL="https://" + app.API_HOST + "/oidc",
                         ACTIONS_ID_TOKEN_REQUEST_TOKEN="synthetic-request-token")):
                token = client.token("test-audience")
            return invoke(action="seal", token=token)
        self.assertEqual(client.reconcile(operation, deadline=1000, reserve=10,
            clock=lambda: 1, sleep=pauses.append), "sealed")
        self.assertEqual(reads, ["token", "token"])
        self.assertEqual(pauses, [5])
        invoke.assert_called_once_with(action="seal", token="synthetic-token")
