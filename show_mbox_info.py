#!/usr/bin/env python3

from __future__ import annotations

import argparse
import email.header
import email.utils
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo


PROGRESS_EVERY = 10000


@dataclass
class MailboxStats:
    total_messages: int = 0
    multipart_messages: int = 0
    attachment_messages: int = 0
    attachment_files: int = 0
    first_dt: datetime | None = None
    last_dt: datetime | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect a Gmail/Google Takeout mbox export and print mailbox stats."
    )
    parser.add_argument("mbox_file", help="Path to the .mbox file to inspect.")
    parser.add_argument(
        "--timezone",
        default="Europe/Stockholm",
        help="Timezone for displayed dates, default: Europe/Stockholm.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top senders/subjects to show, default: 10.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable periodic progress updates to stderr while scanning.",
    )
    return parser.parse_args()


def decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    decoded_parts: list[str] = []
    for chunk, encoding in email.header.decode_header(value):
        if isinstance(chunk, bytes):
            for candidate_encoding in (encoding, "utf-8", "latin-1"):
                if not candidate_encoding:
                    continue
                try:
                    decoded_parts.append(
                        chunk.decode(candidate_encoding, errors="replace")
                    )
                    break
                except LookupError:
                    continue
            else:
                decoded_parts.append(chunk.decode("utf-8", errors="replace"))
        else:
            decoded_parts.append(chunk)
    return "".join(decoded_parts).strip()


def extract_people(values: Iterable[str]) -> list[str]:
    decoded_values = [decode_header_value(value) for value in values if value]
    people: list[str] = []
    for _, address in email.utils.getaddresses(decoded_values):
        people.append(address or "unknown")
    return people


def format_dt(dt: datetime | None, tz_name: str) -> str:
    if dt is None:
        return "n/a"
    return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def parse_headers(header_lines: list[bytes]) -> dict[str, str]:
    unfolded: list[bytes] = []
    current = b""
    for raw_line in header_lines:
        line = raw_line.rstrip(b"\r\n")
        if not line:
            continue
        if line[:1] in (b" ", b"\t") and current:
            current += b" " + line.lstrip()
            continue
        if current:
            unfolded.append(current)
        current = line
    if current:
        unfolded.append(current)

    headers: dict[str, str] = {}
    for line in unfolded:
        if b":" not in line:
            continue
        key, value = line.split(b":", 1)
        headers[key.decode("utf-8", errors="replace").lower()] = value.decode(
            "utf-8", errors="replace"
        ).strip()
    return headers


def update_date_range(stats: MailboxStats, date_header: str | None) -> None:
    if not date_header:
        return
    try:
        parsed_date = email.utils.parsedate_to_datetime(date_header)
    except ValueError:
        try:
            parsed_date = datetime.fromisoformat(date_header)
        except ValueError:
            return
    if parsed_date is None:
        return
    if parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=ZoneInfo("UTC"))
    if stats.first_dt is None or parsed_date < stats.first_dt:
        stats.first_dt = parsed_date
    if stats.last_dt is None or parsed_date > stats.last_dt:
        stats.last_dt = parsed_date


def finalize_message(
    headers: dict[str, str],
    attachment_count: int,
    stats: MailboxStats,
    sender_counts: Counter[str],
    recipient_counts: Counter[str],
    subject_counts: Counter[str],
) -> None:
    if not headers:
        return

    stats.total_messages += 1

    sender = extract_people([headers.get("from", "")])
    sender_counts[sender[0] if sender else "unknown"] += 1

    recipients = extract_people(
        [headers.get("to", ""), headers.get("cc", ""), headers.get("bcc", "")]
    )
    for recipient in recipients:
        if recipient and recipient != "unknown":
            recipient_counts[recipient] += 1

    subject = decode_header_value(headers.get("subject")) or "(no subject)"
    subject_counts[subject] += 1

    update_date_range(stats, headers.get("date"))

    content_type = headers.get("content-type", "").lower()
    if content_type.startswith("multipart/"):
        stats.multipart_messages += 1

    if attachment_count > 0:
        stats.attachment_messages += 1
        stats.attachment_files += attachment_count


def scan_mbox(
    path: Path,
    show_progress: bool,
) -> tuple[MailboxStats, Counter[str], Counter[str], Counter[str]]:
    stats = MailboxStats()
    sender_counts: Counter[str] = Counter()
    recipient_counts: Counter[str] = Counter()
    subject_counts: Counter[str] = Counter()

    current_header_lines: list[bytes] = []
    current_headers: dict[str, str] = {}
    attachment_count = 0
    in_headers = False
    have_message = False
    pending_disposition = b""
    pending_attachment_counted = False

    with path.open("rb") as handle:
        for raw_line in handle:
            if raw_line.startswith(b"From "):
                if have_message:
                    finalize_message(
                        current_headers,
                        attachment_count,
                        stats,
                        sender_counts,
                        recipient_counts,
                        subject_counts,
                    )
                    if show_progress and stats.total_messages % PROGRESS_EVERY == 0:
                        print(
                            f"Scanned {stats.total_messages} messages...",
                            file=sys.stderr,
                            flush=True,
                        )

                current_header_lines = []
                current_headers = {}
                attachment_count = 0
                in_headers = True
                have_message = True
                pending_disposition = b""
                pending_attachment_counted = False
                continue

            if not have_message:
                continue

            if in_headers:
                if raw_line in (b"\n", b"\r\n"):
                    current_headers = parse_headers(current_header_lines)
                    in_headers = False
                    continue
                current_header_lines.append(raw_line)
                continue

            lower_line = raw_line.lower().rstrip(b"\r\n")
            if lower_line.startswith(b"content-disposition:"):
                pending_disposition = lower_line
                pending_attachment_counted = False
                if b"attachment" in pending_disposition:
                    attachment_count += 1
                    pending_attachment_counted = True
                continue

            if pending_disposition and raw_line[:1] in (b" ", b"\t"):
                pending_disposition += b" " + lower_line.lstrip()
                if b"attachment" in pending_disposition and not pending_attachment_counted:
                    attachment_count += 1
                    pending_attachment_counted = True
                continue

            pending_disposition = b""
            pending_attachment_counted = False

    if have_message:
        finalize_message(
            current_headers,
            attachment_count,
            stats,
            sender_counts,
            recipient_counts,
            subject_counts,
        )

    return stats, sender_counts, recipient_counts, subject_counts


def main() -> None:
    args = parse_args()
    path = Path(args.mbox_file)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    stats, sender_counts, recipient_counts, subject_counts = scan_mbox(
        path, show_progress=not args.no_progress
    )

    print(f"File: {path}")
    print(f"Messages: {stats.total_messages}")
    print(
        f"Date range: {format_dt(stats.first_dt, args.timezone)} to "
        f"{format_dt(stats.last_dt, args.timezone)}"
    )
    print(f"Multipart messages: {stats.multipart_messages}")
    print(f"Messages with attachments: {stats.attachment_messages}")
    print(f"Attachment files: {stats.attachment_files}")

    print()
    print(f"Top senders ({args.top}):")
    for sender, count in sender_counts.most_common(args.top):
        print(f"  {count:>6}  {sender}")

    print()
    print(f"Top recipients ({args.top}):")
    for recipient, count in recipient_counts.most_common(args.top):
        print(f"  {count:>6}  {recipient}")

    print()
    print(f"Top subjects ({args.top}):")
    for subject, count in subject_counts.most_common(args.top):
        print(f"  {count:>6}  {subject}")


if __name__ == "__main__":
    main()
