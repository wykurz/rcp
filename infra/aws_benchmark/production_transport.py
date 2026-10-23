"""Production HTTP outage classification without changing allocation retry policy."""

import http.client
import ssl
import urllib.error

from .common import retryable_io


def _certificate_failure(error):
    # inspect only transport wrappers, never classify an arbitrary application's causes
    wrappers = (urllib.error.URLError, ssl.SSLError)
    certificate_errors = (ssl.SSLCertVerificationError,)
    try:
        from botocore.exceptions import SSLError as SdkSSLError
        from urllib3.exceptions import SSLError as PoolSSLError
        from urllib3.util.ssl_match_hostname import CertificateError as HostnameError
    except ImportError:
        pass
    else:
        wrappers += (SdkSSLError, PoolSSLError)
        certificate_errors += (HostnameError,)
    pending, seen = [error], set()
    while pending and len(seen) < 16:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, certificate_errors):
            return True
        if isinstance(current, wrappers):
            children = [current.__cause__, current.__context__, *current.args]
            if isinstance(current, urllib.error.URLError):
                children.append(current.reason)
            values = getattr(current, "kwargs", None)
            if isinstance(values, dict):
                children.append(values.get("error"))
            pending.extend(child for child in children if isinstance(child, BaseException))
    return False


def retryable_transport(error):
    """Retry outages, but never certificate identity or malformed URL failures."""
    if _certificate_failure(error) or isinstance(error, http.client.InvalidURL):
        return False
    if isinstance(error, urllib.error.HTTPError):
        return retryable_io(error)
    return isinstance(error, (OSError, http.client.HTTPException)) or retryable_io(error)
