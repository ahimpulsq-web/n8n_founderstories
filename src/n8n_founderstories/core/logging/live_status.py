from __future__ import annotations

import sys
from typing import Any


class LiveStatusLogger:
    INFO_COLOR = "\033[92m"      # Green
    WARNING_COLOR = "\033[93m"   # Yellow
    CYAN = "\033[96m"
    RESET = "\033[0m"

    def __init__(self, enabled: bool | None = None):
        self._enabled = sys.stderr.isatty() if enabled is None else enabled

        # service -> formatted_line (with ANSI)
        self._lines: dict[str, str] = {}
        # stable order of services as they first appear
        self._order: list[str] = []

        # how many live lines are currently rendered on screen
        self._rendered_line_count = 0

    # -------- public API --------

    def clear_line(self) -> None:
        """Backward compatible: clear the whole live block."""
        self.clear_block()

    def clear_block(self) -> None:
        """Clear the previously rendered live block without touching earlier logs."""
        if not self._enabled or self._rendered_line_count <= 0:
            return

        self._clear_rendered_block()
        sys.stderr.flush()
        self._rendered_line_count = 0

    def reprint_after_log(self) -> None:
        """Reprint the live block after a normal log line is emitted."""
        self._render()

    def update(
        self,
        service: str,
        state: str,
        request_id: str | None = None,
        current: int | None = None,
        total: int | None = None,
        level: str = "INFO",
        **fields: Any,
    ) -> None:
        if not self._enabled:
            return

        if service not in self._lines:
            self._order.append(service)

        self._lines[service] = self._format_line(
            service=service,
            state=state,
            request_id=request_id,
            current=current,
            total=total,
            level=level,
            fields=fields,
        )
        self._render()

    def done(
        self,
        service: str,
        state: str = "COMPLETED",
        request_id: str | None = None,
        level: str = "INFO",
        keep_line: bool = False,
        **fields: Any,
    ) -> None:
        if not self._enabled:
            return

        if service not in self._lines:
            self._order.append(service)

        self._lines[service] = self._format_line(
            service=service,
            state=state,
            request_id=request_id,
            current=None,
            total=None,
            level=level,
            fields=fields,
        )

        # Show final state once (as a normal newline), then optionally remove from live block.
        self._render(final_newline=True)

        if not keep_line:
            self._lines.pop(service, None)
            if service in self._order:
                self._order.remove(service)
            self._render()

    # -------- internal rendering --------

    def _clear_rendered_block(self) -> None:
        """
        Clear the live block safely without erasing historical logs.

        Assumption: after every _render(), the cursor ends on the LAST line of the live block.
        So we move up to the FIRST line of the block and clear from there to end-of-screen.
        """
        n = self._rendered_line_count
        if n <= 0:
            return

        if n > 1:
            sys.stderr.write(f"\033[{n-1}A")  # move up to the first line of the block
        sys.stderr.write("\r\033[J")         # clear from cursor to end of screen

    def _render(self, final_newline: bool = False) -> None:
        """Redraw the entire live block in-place."""
        if not self._enabled:
            return

        if self._rendered_line_count > 0:
            self._clear_rendered_block()

        lines = [self._lines[s] for s in self._order if s in self._lines]
        if not lines:
            self._rendered_line_count = 0
            sys.stderr.flush()
            return

        sys.stderr.write("\n".join(lines))
        if final_newline:
            sys.stderr.write("\n")
        sys.stderr.flush()

        self._rendered_line_count = len(lines)

    def _format_line(
        self,
        service: str,
        state: str,
        request_id: str | None,
        current: int | None,
        total: int | None,
        level: str,
        fields: dict[str, Any],
    ) -> str:
        parts = [service, f"STATE={state}"]

        if request_id:
            parts.append(f"request_id={request_id}")

        if current is not None and total is not None:
            parts.append(f"{current}/{total}")

        priority_keys = [
            "completed_total", "location", "locations", "loc", "headcount", "kw", "query", "page",
            "results", "domains", "found", "status", "wait_s", "wait_left_s", "attempt", "http_status"
        ]
        for key in priority_keys:
            if key in fields and fields[key] is not None:
                parts.append(f"{key}={self._format_value(key, fields[key])}")

        remaining_keys = sorted(set(fields.keys()) - set(priority_keys))
        for key in remaining_keys:
            if fields[key] is not None:
                parts.append(f"{key}={self._format_value(key, fields[key])}")

        message_parts = " | ".join(parts[1:])

        level_name = (level or "INFO").upper()
        color = self.INFO_COLOR if level_name == "INFO" else self.WARNING_COLOR
        padded_level = f"{color}{level_name:<8}{self.RESET}"

        return f"{padded_level} {self.CYAN}{service}{self.RESET} | {message_parts}"

    def _format_value(self, key: str, value: Any) -> str:
        if value is None:
            return ""
        if key == "query":
            s = str(value)
            return repr(s) if " " in s else s
        s = str(value)
        return s if len(s) <= 100 else s[:97] + "..."
