"""Single-run lock so two overlapping runs can't write the same manifest/database.

A daily job whose catalog call can take ~90s, plus a manual re-run, is exactly the
situation that produces overlap. The lock is an OS-level lock on `state/run.lock`
(fcntl.flock on POSIX, msvcrt.locking on Windows), not the file's existence. The OS
drops it when the holding process exits for any reason, kill -9 included, so a
crashed run can never wedge the schedule and there is no "stale lock" age to guess.

The file itself is left in place on release: deleting a lock file that another
process may have just opened is how two runs end up both believing they hold it.
"""

from __future__ import annotations

import os
from pathlib import Path

if os.name == "nt":
    import msvcrt

    def _try_lock(fd: int) -> bool:
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # lock byte 0, fail at once if taken
        except OSError:
            return False
        return True

    def _unlock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        return True

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


class LockHeldError(RuntimeError):
    pass


class RunLock:
    def __init__(self, path: str | os.PathLike) -> None:
        self.path = Path(path)
        self._fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR)
        if not _try_lock(fd):
            os.close(fd)
            raise LockHeldError(f"another run holds {self.path}")
        # Record the holder for whoever looks at the file; the lock is what counts.
        # Written past byte 0 so it never touches the locked region on Windows.
        os.ftruncate(fd, 1)
        os.lseek(fd, 1, os.SEEK_SET)
        os.write(fd, f"pid={os.getpid()}\n".encode())
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            _unlock(fd)
        finally:
            os.close(fd)

    def __enter__(self) -> "RunLock":
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()
