"""
notifications.py
================
Email admins when the Enrollment Engine fails. Designed to be called from
a top-level try/except in engine.py so the alert includes:

    * the target environment (beta vs production)
    * the stage the engine was in when it failed
    * the full Python traceback

Example:
    from notifications import Notifier
    notifier = Notifier(config["smtp"], env=config["TARGET_ENV"])
    try:
        run()
    except Exception:
        notifier.send_failure(stage="loader:fyr", exc_info=sys.exc_info())
        raise
"""

from __future__ import annotations

import logging
import os
import smtplib
import socket
import traceback
from email.message import EmailMessage
from types import TracebackType
from typing import Sequence

log = logging.getLogger(__name__)


class Notifier:
    def __init__(self, smtp_cfg: dict, env: str):
        self.cfg = smtp_cfg
        self.env = env

    # ------------------------------------------------------------------ api
    def send_failure(
        self,
        stage: str,
        exc_info: tuple[type[BaseException], BaseException, TracebackType] | None = None,
        extra: str | None = None,
    ) -> bool:
        subject = (
            f"{self.cfg.get('subject_prefix', '[Enrollment Engine]')} "
            f"FAILURE in {self.env.upper()} at stage: {stage}"
        )
        body_lines = [
            f"The Enrollment Engine failed while running against: {self.env.upper()}",
            f"Host:  {socket.gethostname()}",
            f"Stage: {stage}",
            "",
        ]
        if extra:
            body_lines += ["Context:", extra, ""]
        if exc_info and exc_info[0] is not None:
            body_lines += ["Traceback:", "".join(traceback.format_exception(*exc_info))]
        else:
            body_lines += ["(no traceback captured)"]

        return self._send(subject, "\n".join(body_lines), self.cfg["to_addresses"])

    def send_info(self, subject: str, body: str) -> bool:
        full_subject = f"{self.cfg.get('subject_prefix', '[Enrollment Engine]')} {subject}"
        return self._send(full_subject, body, self.cfg["to_addresses"])

    # -------------------------------------------------------------- internal
    def _send(self, subject: str, body: str, to: Sequence[str]) -> bool:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.cfg["from_address"]
        msg["To"] = ", ".join(to)
        msg.set_content(body)

        host = self.cfg["host"]
        port = int(self.cfg.get("port", 587))
        use_tls = bool(self.cfg.get("use_tls", True))
        username = self.cfg.get("username")
        password = os.environ.get(self.cfg.get("password_env_var", ""), "")

        try:
            with smtplib.SMTP(host, port, timeout=30) as smtp:
                smtp.ehlo()
                if use_tls:
                    smtp.starttls()
                    smtp.ehlo()
                if username and password:
                    smtp.login(username, password)
                smtp.send_message(msg)
            log.info("Notification sent: %s", subject)
            return True
        except Exception as e:
            # Never let the notifier itself crash the engine's error path.
            log.error("Failed to send notification (%s): %s", subject, e)
            return False
