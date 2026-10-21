"""Defer signals through resource cleanup without raising inside library locks."""

import signal
import threading


class CancelledSignal(Exception):
    """Cooperative unwind while the deferred signal still owns its handler."""


class SignalCancellation:
    def __init__(self):
        self.stopped = False
        self.pending = None
        self.previous = {}

    def __enter__(self):
        if threading.current_thread() is threading.main_thread():
            # save every handler before installing either one, including install failures
            self.previous = {number: signal.getsignal(number)
                             for number in (signal.SIGINT, signal.SIGTERM)}
            try:
                for number, previous in self.previous.items():
                    if previous != signal.SIG_IGN:
                        signal.signal(number, self._record)
            except BaseException:
                self.__exit__(None, None, None)
                raise
        return self

    def _record(self, number, _frame):
        # handlers may run inside executor/threading locks: plain state only
        if self.pending is None:
            self.pending = number
        self.stopped = True

    def checkpoint(self):
        if self.stopped:
            raise CancelledSignal()

    def __exit__(self, _type, _value, _traceback):
        for number, handler in self.previous.items():
            signal.signal(number, handler)
        if self.pending is not None:
            number, self.pending = self.pending, None
            previous = self.previous.get(number)
            if callable(previous):
                previous(number, None)
                # an outer guard must finish its cleanup before chaining the user's handler
                if isinstance(getattr(previous, "__self__", None), SignalCancellation):
                    raise CancelledSignal()
            if number == signal.SIGINT:
                raise KeyboardInterrupt()
            raise InterruptedError(signal.Signals(number).name)
        return False
