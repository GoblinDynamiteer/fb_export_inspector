#!/usr/bin/env python3

from __future__ import annotations

import argparse
import email.header
import email.parser
import email.policy
import email.utils
import html
import quopri
import re
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo


PROGRESS_EVERY = 10000
COMMIT_EVERY = 1000
SNIPPET_LIMIT = 220
DEFAULT_MAX_BODY_CHARS = 50000
BASE64_CHARS = set(
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
)
ANSI_RESET = "\033[0m"
ANSI_HIGHLIGHT = "\033[1;30;43m"


@dataclass
class MailboxStats:
    total_messages: int = 0
    multipart_messages: int = 0
    attachment_messages: int = 0
    attachment_files: int = 0
    first_dt: datetime | None = None
    last_dt: datetime | None = None


@dataclass
class SearchResult:
    index: int
    date_text: str
    sender: str
    to_text: str
    subject: str
    labels: str
    thread_id: str
    snippet: str


@dataclass
class IndexStats:
    multipart_messages: int = 0
    attachment_messages: int = 0
    attachment_files: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect, search, and index Gmail/Google Takeout mailbox exports."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    info_parser = subparsers.add_parser(
        "info", help="Show mailbox stats for a raw .mbox file or SQLite index."
    )
    info_parser.add_argument("input_file", help="Path to the .mbox or .sqlite file.")
    info_parser.add_argument(
        "--timezone",
        default="Europe/Stockholm",
        help="Timezone for displayed dates, default: Europe/Stockholm.",
    )
    info_parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top senders/subjects to show, default: 10.",
    )
    info_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable periodic progress updates to stderr while scanning raw .mbox input.",
    )

    search_parser = subparsers.add_parser(
        "search", help="Search a raw .mbox file or SQLite mailbox index."
    )
    search_parser.add_argument("input_file", help="Path to the .mbox or .sqlite file.")
    search_parser.add_argument(
        "terms",
        nargs="*",
        help="Case-insensitive search terms. By default all terms must match.",
    )
    search_parser.add_argument(
        "--from",
        dest="from_filter",
        help="Only include messages whose sender contains this text.",
    )
    search_parser.add_argument(
        "--to",
        dest="to_filter",
        help="Only include messages whose recipients contain this text.",
    )
    search_parser.add_argument(
        "--subject",
        dest="subject_filter",
        help="Only include messages whose subject contains this text.",
    )
    search_parser.add_argument(
        "--label",
        dest="label_filter",
        help="Only include messages whose labels contain this text.",
    )
    search_parser.add_argument(
        "--after",
        help="Only include messages on or after this date (YYYY-MM-DD).",
    )
    search_parser.add_argument(
        "--before",
        help="Only include messages before this date (YYYY-MM-DD).",
    )
    search_parser.add_argument(
        "--any",
        action="store_true",
        help="Match if any search term is found instead of requiring all terms.",
    )
    search_parser.add_argument(
        "--headers-only",
        action="store_true",
        help="Search only headers, not message bodies. Supported only for raw .mbox input.",
    )
    search_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results to print, default: 20.",
    )
    search_parser.add_argument(
        "--timezone",
        default="Europe/Stockholm",
        help="Timezone for displayed dates, default: Europe/Stockholm.",
    )
    search_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable periodic progress updates to stderr while scanning raw .mbox input.",
    )
    search_parser.add_argument(
        "--no-ansi",
        action="store_true",
        help="Disable ANSI highlight output in search results.",
    )

    show_parser = subparsers.add_parser(
        "show", help="Display one full message by message index."
    )
    show_parser.add_argument(
        "input_file",
        help="Path to the .mbox file or .sqlite mailbox index.",
    )
    show_parser.add_argument(
        "message_index",
        type=int,
        help="Message index from search results.",
    )
    show_parser.add_argument(
        "--timezone",
        default="Europe/Stockholm",
        help="Timezone for displayed dates, default: Europe/Stockholm.",
    )
    show_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable periodic progress updates to stderr while scanning raw .mbox input.",
    )

    index_parser = subparsers.add_parser(
        "index", help="Build a SQLite full-text index for a raw .mbox file."
    )
    index_parser.add_argument("mbox_file", help="Path to the source .mbox file.")
    index_parser.add_argument(
        "index_file",
        nargs="?",
        default="gmail_index.sqlite",
        help="Path to the SQLite index to create, default: gmail_index.sqlite.",
    )
    index_parser.add_argument(
        "--max-body-chars",
        type=int,
        default=DEFAULT_MAX_BODY_CHARS,
        help="Maximum body characters to keep per message in the search index.",
    )
    index_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable periodic progress updates to stderr while indexing.",
    )
    index_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output index file if it already exists.",
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


def extract_addresses(*values: str) -> str:
    decoded_values = [decode_header_value(value) for value in values if value]
    addresses = [address or "unknown" for _, address in email.utils.getaddresses(decoded_values)]
    return ", ".join(addresses)


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


def parse_date(date_header: str | None) -> datetime | None:
    if not date_header:
        return None
    try:
        parsed_date = email.utils.parsedate_to_datetime(date_header)
    except ValueError:
        try:
            parsed_date = datetime.fromisoformat(date_header)
        except ValueError:
            return None
    if parsed_date is None:
        return None
    if parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=ZoneInfo("UTC"))
    return parsed_date


def parse_date_filter(value: str | None, tz_name: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=ZoneInfo(tz_name))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD."
        ) from exc


def format_date(dt: datetime | None, tz_name: str) -> str:
    if dt is None:
        return "n/a"
    return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def shorten(text: str, limit: int = SNIPPET_LIMIT) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def use_ansi(args: argparse.Namespace) -> bool:
    return sys.stdout.isatty() and not getattr(args, "no_ansi", False)


def looks_like_encoded_blob(raw_line: bytes) -> bool:
    stripped = raw_line.strip()
    if len(stripped) < 80:
        return False
    if b" " in stripped or b"\t" in stripped:
        return False
    return all(byte in BASE64_CHARS for byte in stripped)


def line_text_candidates(raw_line: bytes) -> list[str]:
    stripped = raw_line.strip()
    if not stripped or looks_like_encoded_blob(stripped):
        return []

    candidates: list[str] = []
    decoded = stripped.decode("utf-8", errors="replace")
    candidates.append(decoded)

    qp_decoded = quopri.decodestring(stripped)
    qp_text = qp_decoded.decode("utf-8", errors="replace")
    if qp_text != decoded:
        candidates.append(qp_text)

    return candidates


def consume_body_line(
    raw_line: bytes,
    pending_qp_line: bytes,
) -> tuple[list[str], bytes]:
    stripped = raw_line.rstrip(b"\r\n")
    if pending_qp_line:
        stripped = pending_qp_line + stripped
        pending_qp_line = b""

    # Quoted-printable soft line breaks often split words across lines, which can
    # create false matches like "b=\norder-collapse" for the search term "order".
    if stripped.endswith(b"="):
        return [], stripped[:-1]

    return line_text_candidates(stripped), pending_qp_line


def choose_text_candidate(candidates: list[str]) -> str:
    if not candidates:
        return ""
    for candidate in candidates:
        compact = " ".join(candidate.split())
        if compact:
            return compact
    return ""


def term_matches_text(term: str, text: str) -> bool:
    if " " in term:
        return term in text
    if all(char.isalnum() for char in term):
        return re.search(rf"(?<!\w){re.escape(term)}\w*", text) is not None
    return term in text


def highlight_text(text: str, terms: list[str], ansi_enabled: bool) -> str:
    if not text:
        return text

    matches: list[tuple[int, int]] = []
    lowered = text.lower()
    for term in sorted({term for term in terms if term}, key=len, reverse=True):
        if " " in term:
            pattern = re.escape(term)
        elif all(char.isalnum() for char in term):
            pattern = rf"(?<!\w){re.escape(term)}\w*"
        else:
            pattern = re.escape(term)
        for match in re.finditer(pattern, lowered):
            matches.append((match.start(), match.end()))

    if not matches:
        return text

    matches.sort()
    merged: list[tuple[int, int]] = []
    for start, end in matches:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))

    parts: list[str] = []
    cursor = 0
    for start, end in merged:
        parts.append(text[cursor:start])
        hit = text[start:end]
        if ansi_enabled:
            parts.append(f"{ANSI_HIGHLIGHT}{hit}{ANSI_RESET}")
        else:
            parts.append(f"[{hit}]")
        cursor = end
    parts.append(text[cursor:])
    return "".join(parts)


def terms_match_in_texts(
    terms: list[str],
    texts: list[str],
    match_any: bool,
) -> tuple[set[str], str | None]:
    matched: set[str] = set()
    snippet: str | None = None
    for text in texts:
        lowered = text.lower()
        line_matched = [term for term in terms if term_matches_text(term, lowered)]
        if not line_matched:
            continue
        matched.update(line_matched)
        if snippet is None:
            snippet = shorten(text)
        if match_any:
            break
    return matched, snippet


def detect_sqlite(path: Path) -> bool:
    if path.suffix.lower() in {".sqlite", ".db", ".sqlite3"}:
        return True
    with path.open("rb") as handle:
        return handle.read(16) == b"SQLite format 3\x00"


def load_sqlite_metadata(path: Path) -> dict[str, str]:
    conn = sqlite3.connect(path)
    try:
        return load_metadata(conn)
    finally:
        conn.close()


def update_date_range(stats: MailboxStats, date_header: str | None) -> None:
    parsed_date = parse_date(date_header)
    if parsed_date is None:
        return
    if stats.first_dt is None or parsed_date < stats.first_dt:
        stats.first_dt = parsed_date
    if stats.last_dt is None or parsed_date > stats.last_dt:
        stats.last_dt = parsed_date


def finalize_info_message(
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


def scan_mbox_info(
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
                    finalize_info_message(
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
        finalize_info_message(
            current_headers,
            attachment_count,
            stats,
            sender_counts,
            recipient_counts,
            subject_counts,
        )

    return stats, sender_counts, recipient_counts, subject_counts


def load_metadata(conn: sqlite3.Connection) -> dict[str, str]:
    try:
        rows = conn.execute("SELECT key, value FROM metadata").fetchall()
    except sqlite3.OperationalError:
        return {}
    return {key: value for key, value in rows}


def scan_sqlite_info(
    path: Path,
) -> tuple[MailboxStats, Counter[str], Counter[str], Counter[str], bool]:
    conn = sqlite3.connect(path)
    metadata = load_metadata(conn)

    sender_counts = Counter(
        {
            sender: count
            for sender, count in conn.execute(
                "SELECT sender, COUNT(*) FROM messages GROUP BY sender"
            )
        }
    )
    recipient_counts = Counter(
        {
            recipient: count
            for recipient, count in conn.execute(
                """
                SELECT trim(value), COUNT(*)
                FROM messages, json_each('["' || replace(replace(recipients, '"', '""'), ', ', '","') || '"]')
                WHERE recipients IS NOT NULL AND recipients != ''
                GROUP BY trim(value)
                """
            )
            if recipient
        }
    )
    subject_counts = Counter(
        {
            subject: count
            for subject, count in conn.execute(
                "SELECT subject, COUNT(*) FROM messages GROUP BY subject"
            )
        }
    )
    row = conn.execute(
        "SELECT MIN(date_utc), MAX(date_utc), COUNT(*) FROM messages"
    ).fetchone()
    conn.close()

    def metadata_int(key: str) -> int:
        value = metadata.get(key)
        return int(value) if value is not None else 0

    first_dt = datetime.fromisoformat(row[0]) if row and row[0] else None
    last_dt = datetime.fromisoformat(row[1]) if row and row[1] else None
    total_messages = int(row[2]) if row and row[2] is not None else 0
    stats = MailboxStats(
        total_messages=total_messages,
        multipart_messages=metadata_int("multipart_messages"),
        attachment_messages=metadata_int("attachment_messages"),
        attachment_files=metadata_int("attachment_files"),
        first_dt=first_dt,
        last_dt=last_dt,
    )
    has_mime_stats = all(
        key in metadata
        for key in ("multipart_messages", "attachment_messages", "attachment_files")
    )
    return stats, sender_counts, recipient_counts, subject_counts, has_mime_stats


def print_info(
    path: Path,
    stats: MailboxStats,
    sender_counts: Counter[str],
    recipient_counts: Counter[str],
    subject_counts: Counter[str],
    top: int,
    timezone: str,
    has_mime_stats: bool,
) -> None:
    print(f"File: {path}")
    print(f"Messages: {stats.total_messages}")
    print(
        f"Date range: {format_date(stats.first_dt, timezone)} to "
        f"{format_date(stats.last_dt, timezone)}"
    )
    if has_mime_stats:
        print(f"Multipart messages: {stats.multipart_messages}")
        print(f"Messages with attachments: {stats.attachment_messages}")
        print(f"Attachment files: {stats.attachment_files}")
    else:
        print("Multipart messages: n/a (not stored in this index)")
        print("Messages with attachments: n/a (not stored in this index)")
        print("Attachment files: n/a (not stored in this index)")

    print()
    print(f"Top senders ({top}):")
    for sender, count in sender_counts.most_common(top):
        print(f"  {count:>6}  {sender}")

    print()
    print(f"Top recipients ({top}):")
    for recipient, count in recipient_counts.most_common(top):
        print(f"  {count:>6}  {recipient}")

    print()
    print(f"Top subjects ({top}):")
    for subject, count in subject_counts.most_common(top):
        print(f"  {count:>6}  {subject}")


def build_header_search_fields(headers: dict[str, str]) -> list[tuple[str, str]]:
    fields = [
        ("From", decode_header_value(headers.get("from"))),
        ("To", decode_header_value(headers.get("to"))),
        ("Cc", decode_header_value(headers.get("cc"))),
        ("Subject", decode_header_value(headers.get("subject"))),
        ("Labels", decode_header_value(headers.get("x-gmail-labels"))),
    ]
    return [(name, value) for name, value in fields if value]


def message_passes_filters(
    headers: dict[str, str],
    parsed_date: datetime | None,
    args: argparse.Namespace,
) -> bool:
    sender = decode_header_value(headers.get("from")).lower()
    recipients = " ".join(
        [
            decode_header_value(headers.get("to")).lower(),
            decode_header_value(headers.get("cc")).lower(),
            decode_header_value(headers.get("bcc")).lower(),
        ]
    )
    subject = decode_header_value(headers.get("subject")).lower()
    labels = decode_header_value(headers.get("x-gmail-labels")).lower()

    if args.from_filter and args.from_filter.lower() not in sender:
        return False
    if args.to_filter and args.to_filter.lower() not in recipients:
        return False
    if args.subject_filter and args.subject_filter.lower() not in subject:
        return False
    if args.label_filter and args.label_filter.lower() not in labels:
        return False

    after_dt = parse_date_filter(args.after, args.timezone)
    before_dt = parse_date_filter(args.before, args.timezone)
    if after_dt and (parsed_date is None or parsed_date.astimezone(after_dt.tzinfo) < after_dt):
        return False
    if before_dt and (parsed_date is None or parsed_date.astimezone(before_dt.tzinfo) >= before_dt):
        return False

    return True


def finalize_search_message(
    index: int,
    headers: dict[str, str],
    body_match_terms: set[str],
    body_snippet: str | None,
    args: argparse.Namespace,
) -> SearchResult | None:
    if not headers:
        return None

    parsed_date = parse_date(headers.get("date"))
    if not message_passes_filters(headers, parsed_date, args):
        return None

    sender = decode_header_value(headers.get("from")) or "unknown"
    to_text = decode_header_value(headers.get("to"))
    subject = decode_header_value(headers.get("subject")) or "(no subject)"
    labels = decode_header_value(headers.get("x-gmail-labels"))
    thread_id = headers.get("x-gm-thrid", "")

    query_terms = [term.lower() for term in args.terms]
    if not query_terms:
        matched_enough = True
        snippet = body_snippet or shorten(subject)
    else:
        header_fields = build_header_search_fields(headers)
        header_texts = [f"{name}: {value}" for name, value in header_fields]
        header_matches, header_snippet = terms_match_in_texts(
            query_terms, header_texts, args.any
        )
        matched_terms = set(header_matches)
        matched_terms.update(body_match_terms)
        matched_enough = bool(matched_terms) if args.any else len(matched_terms) == len(query_terms)
        snippet = header_snippet or body_snippet or shorten(subject)

    if not matched_enough:
        return None

    return SearchResult(
        index=index,
        date_text=format_date(parsed_date, args.timezone),
        sender=sender,
        to_text=to_text,
        subject=subject,
        labels=labels,
        thread_id=thread_id,
        snippet=highlight_text(snippet, query_terms, use_ansi(args)),
    )


def search_mbox(args: argparse.Namespace) -> list[SearchResult]:
    path = Path(args.input_file)
    results: list[SearchResult] = []

    current_header_lines: list[bytes] = []
    current_headers: dict[str, str] = {}
    in_headers = False
    have_message = False
    index = 0

    query_terms = [term.lower() for term in args.terms]
    current_body_match_terms: set[str] = set()
    current_body_snippet: str | None = None
    current_need_body_scan = False
    pending_qp_line = b""

    with path.open("rb") as handle:
        for raw_line in handle:
            if raw_line.startswith(b"From "):
                if have_message:
                    index += 1
                    result = finalize_search_message(
                        index,
                        current_headers,
                        current_body_match_terms,
                        current_body_snippet,
                        args,
                    )
                    if result:
                        results.append(result)
                        if len(results) >= args.limit:
                            return results
                    if not args.no_progress and index % PROGRESS_EVERY == 0:
                        print(
                            f"Scanned {index} messages...",
                            file=sys.stderr,
                            flush=True,
                        )

                current_header_lines = []
                current_headers = {}
                current_body_match_terms = set()
                current_body_snippet = None
                current_need_body_scan = bool(query_terms) and not args.headers_only
                pending_qp_line = b""
                in_headers = True
                have_message = True
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

            if not current_need_body_scan or not query_terms:
                continue

            candidates, pending_qp_line = consume_body_line(raw_line, pending_qp_line)
            if not candidates:
                continue
            matched_terms, snippet = terms_match_in_texts(query_terms, candidates, args.any)
            if matched_terms:
                current_body_match_terms.update(matched_terms)
                if current_body_snippet is None and snippet:
                    current_body_snippet = snippet
                if args.any or len(current_body_match_terms) == len(query_terms):
                    current_need_body_scan = False

    if have_message:
        index += 1
        result = finalize_search_message(
            index,
            current_headers,
            current_body_match_terms,
            current_body_snippet,
            args,
        )
        if result:
            results.append(result)

    return results


def build_fts_query(terms: list[str], match_any: bool) -> str:
    escaped: list[str] = []
    for term in terms:
        term = term.strip()
        if not term:
            continue
        if " " not in term and all(char.isalnum() for char in term):
            escaped.append(f"{term.replace('"', '""')}*")
        else:
            escaped_term = term.replace('"', '""')
            escaped.append(f'"{escaped_term}"')
    if not escaped:
        return ""
    operator = " OR " if match_any else " AND "
    return operator.join(escaped)


def search_sqlite(args: argparse.Namespace) -> list[SearchResult]:
    conn = sqlite3.connect(args.input_file)
    conn.row_factory = sqlite3.Row

    where_clauses: list[str] = []
    params: list[object] = []

    fts_query = build_fts_query(args.terms, args.any)
    from_clause = "messages"
    if fts_query:
        from_clause = "messages JOIN message_fts ON message_fts.rowid = messages.id"
        where_clauses.append("message_fts MATCH ?")
        params.append(fts_query)

    if args.from_filter:
        where_clauses.append("lower(messages.sender) LIKE ?")
        params.append(f"%{args.from_filter.lower()}%")
    if args.to_filter:
        where_clauses.append("lower(messages.recipients) LIKE ?")
        params.append(f"%{args.to_filter.lower()}%")
    if args.subject_filter:
        where_clauses.append("lower(messages.subject) LIKE ?")
        params.append(f"%{args.subject_filter.lower()}%")
    if args.label_filter:
        where_clauses.append("lower(messages.labels) LIKE ?")
        params.append(f"%{args.label_filter.lower()}%")

    after_dt = parse_date_filter(args.after, args.timezone)
    before_dt = parse_date_filter(args.before, args.timezone)
    if after_dt is not None:
        where_clauses.append("messages.date_unix >= ?")
        params.append(int(after_dt.timestamp()))
    if before_dt is not None:
        where_clauses.append("messages.date_unix < ?")
        params.append(int(before_dt.timestamp()))

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    snippet_sql = "messages.snippet"
    if fts_query:
        snippet_sql = "snippet(message_fts, -1, '', '', ' ... ', 18)"

    query = f"""
        SELECT
            messages.message_index,
            messages.date_utc,
            messages.sender,
            messages.recipients,
            messages.subject,
            messages.labels,
            messages.thread_id,
            {snippet_sql} AS snippet
        FROM {from_clause}
        {where_sql}
        ORDER BY messages.date_unix DESC, messages.message_index DESC
        LIMIT ?
    """
    params.append(args.limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    ansi_enabled = use_ansi(args)
    query_terms = [term.lower() for term in args.terms]

    return [
        SearchResult(
            index=row["message_index"],
            date_text=format_date(
                datetime.fromisoformat(row["date_utc"]) if row["date_utc"] else None,
                args.timezone,
            ),
            sender=row["sender"],
            to_text=row["recipients"] or "",
            subject=row["subject"],
            labels=row["labels"] or "",
            thread_id=row["thread_id"] or "",
            snippet=highlight_text(row["snippet"] or "", query_terms, ansi_enabled),
        )
        for row in rows
    ]


def print_results(results: list[SearchResult]) -> None:
    if not results:
        print("No matches found.")
        return
    for result in results:
        print(f"{result.index}. {result.date_text}")
        print(f"   From: {result.sender}")
        if result.to_text:
            print(f"   To: {result.to_text}")
        print(f"   Subject: {result.subject}")
        if result.labels:
            print(f"   Labels: {result.labels}")
        if result.thread_id:
            print(f"   Thread: {result.thread_id}")
        print(f"   Snippet: {result.snippet}")
        print()


def collect_message_body_text(body_lines: list[bytes]) -> str:
    parts: list[str] = []
    pending_qp_line = b""
    for raw_line in body_lines:
        candidates, pending_qp_line = consume_body_line(raw_line, pending_qp_line)
        text = choose_text_candidate(candidates)
        if text:
            parts.append(text)
    if pending_qp_line:
        text = choose_text_candidate(line_text_candidates(pending_qp_line))
        if text:
            parts.append(text)
    return "\n".join(parts).strip()


def html_to_text(value: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def decode_payload(payload: bytes, charset: str | None) -> str:
    for candidate in (charset, "utf-8", "latin-1"):
        if not candidate:
            continue
        try:
            return payload.decode(candidate, errors="replace")
        except LookupError:
            continue
    return payload.decode("utf-8", errors="replace")


def extract_message_body(message) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    for part in message.walk():
        if part.get_content_disposition() == "attachment":
            continue
        if part.get_content_maintype() != "text":
            continue

        payload = part.get_payload(decode=True)
        if payload is None:
            raw_payload = part.get_payload()
            if isinstance(raw_payload, str):
                text = raw_payload
            else:
                continue
        else:
            text = decode_payload(payload, part.get_content_charset())

        subtype = part.get_content_subtype().lower()
        if subtype == "plain":
            plain_parts.append(text)
        elif subtype == "html":
            html_parts.append(html_to_text(text))

    if plain_parts:
        return "\n\n".join(part.strip() for part in plain_parts if part.strip()).strip()
    if html_parts:
        return "\n\n".join(part.strip() for part in html_parts if part.strip()).strip()
    return ""


def parse_message_bytes(message_bytes: bytes) -> tuple[dict[str, str], str]:
    message = email.parser.BytesParser(policy=email.policy.default).parsebytes(
        message_bytes
    )
    headers = {
        key.lower(): str(value)
        for key, value in message.items()
    }
    body_text = extract_message_body(message)
    return headers, body_text


def format_full_message(headers: dict[str, str], body_text: str, timezone: str) -> str:
    sender = decode_header_value(headers.get("from")) or "unknown"
    recipients = decode_header_value(headers.get("to"))
    cc_text = decode_header_value(headers.get("cc"))
    subject = decode_header_value(headers.get("subject")) or "(no subject)"
    labels = decode_header_value(headers.get("x-gmail-labels"))
    thread_id = headers.get("x-gm-thrid", "")
    message_id = decode_header_value(headers.get("message-id"))
    parsed_date = parse_date(headers.get("date"))
    lines = [
        f"Date: {format_date(parsed_date, timezone)}",
        f"From: {sender}",
    ]
    if recipients:
        lines.append(f"To: {recipients}")
    if cc_text:
        lines.append(f"Cc: {cc_text}")
    lines.append(f"Subject: {subject}")
    if labels:
        lines.append(f"Labels: {labels}")
    if thread_id:
        lines.append(f"Thread: {thread_id}")
    if message_id:
        lines.append(f"Message-Id: {message_id}")
    lines.append("")
    lines.append(body_text or "(no decoded body text)")
    return "\n".join(lines)


def load_message_from_mbox(
    path: Path,
    target_index: int,
    show_progress: bool,
) -> tuple[dict[str, str], str]:
    current_header_lines: list[bytes] = []
    current_headers: dict[str, str] = {}
    current_body_lines: list[bytes] = []
    in_headers = False
    have_message = False
    index = 0

    with path.open("rb") as handle:
        for raw_line in handle:
            if raw_line.startswith(b"From "):
                if have_message:
                    index += 1
                    if index == target_index:
                        message_bytes = b"".join(current_header_lines) + b"\n" + b"".join(
                            current_body_lines
                        )
                        return parse_message_bytes(message_bytes)
                    if show_progress and index % PROGRESS_EVERY == 0:
                        print(
                            f"Scanned {index} messages...",
                            file=sys.stderr,
                            flush=True,
                        )

                current_header_lines = []
                current_headers = {}
                current_body_lines = []
                in_headers = True
                have_message = True
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

            current_body_lines.append(raw_line)

    if have_message:
        index += 1
        if index == target_index:
            message_bytes = b"".join(current_header_lines) + b"\n" + b"".join(
                current_body_lines
            )
            return parse_message_bytes(message_bytes)

    raise SystemExit(f"Message index not found: {target_index}")


def load_message_from_offset(path: Path, source_offset: int) -> tuple[dict[str, str], str]:
    with path.open("rb") as handle:
        handle.seek(source_offset)
        first_line = handle.readline()
        if not first_line.startswith(b"From "):
            raise SystemExit(f"Invalid message offset in index: {source_offset}")

        message_lines: list[bytes] = []
        while True:
            position = handle.tell()
            raw_line = handle.readline()
            if not raw_line:
                break
            if raw_line.startswith(b"From "):
                handle.seek(position)
                break
            message_lines.append(raw_line)

    return parse_message_bytes(b"".join(message_lines))


def open_database(path: Path, force: bool) -> sqlite3.Connection:
    if path.exists():
        if not force:
            raise SystemExit(
                f"Index file already exists: {path}. Use --force to overwrite it."
            )
        path.unlink()

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    conn.execute("PRAGMA cache_size=-200000")
    conn.executescript(
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            message_index INTEGER NOT NULL UNIQUE,
            source_path TEXT NOT NULL,
            source_offset INTEGER,
            thread_id TEXT,
            gmail_message_id TEXT,
            message_id TEXT,
            date_utc TEXT,
            date_unix INTEGER,
            sender TEXT,
            recipients TEXT,
            subject TEXT,
            labels TEXT,
            snippet TEXT
        );

        CREATE VIRTUAL TABLE message_fts USING fts5(
            sender,
            recipients,
            subject,
            labels,
            body
        );

        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    return conn


def finalize_index_message(
    conn: sqlite3.Connection,
    message_index: int,
    source_path: str,
    source_offset: int,
    headers: dict[str, str],
    body_parts: list[str],
    stats: IndexStats,
) -> None:
    if not headers:
        return

    sender = decode_header_value(headers.get("from")) or "unknown"
    recipients = extract_addresses(
        headers.get("to", ""), headers.get("cc", ""), headers.get("bcc", "")
    )
    subject = decode_header_value(headers.get("subject")) or "(no subject)"
    labels = decode_header_value(headers.get("x-gmail-labels"))
    thread_id = headers.get("x-gm-thrid", "")
    gmail_message_id = headers.get("x-gm-msgid", "")
    message_id = decode_header_value(headers.get("message-id"))
    parsed_date = parse_date(headers.get("date"))
    if parsed_date is not None:
        parsed_date = parsed_date.astimezone(UTC)
    date_utc = parsed_date.isoformat() if parsed_date else None
    date_unix = int(parsed_date.timestamp()) if parsed_date else None

    content_type = headers.get("content-type", "").lower()
    if content_type.startswith("multipart/"):
        stats.multipart_messages += 1

    attachment_count = int(headers.get("__attachment_count__", "0") or "0")
    if attachment_count > 0:
        stats.attachment_messages += 1
        stats.attachment_files += attachment_count

    body_text = "\n".join(part for part in body_parts if part)
    snippet = shorten(body_text or subject)

    cursor = conn.execute(
        """
        INSERT INTO messages (
            message_index,
            source_path,
            source_offset,
            thread_id,
            gmail_message_id,
            message_id,
            date_utc,
            date_unix,
            sender,
            recipients,
            subject,
            labels,
            snippet
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            message_index,
            source_path,
            source_offset,
            thread_id,
            gmail_message_id,
            message_id,
            date_utc,
            date_unix,
            sender,
            recipients,
            subject,
            labels,
            snippet,
        ),
    )
    conn.execute(
        """
        INSERT INTO message_fts(rowid, sender, recipients, subject, labels, body)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (cursor.lastrowid, sender, recipients, subject, labels, body_text),
    )


def index_mbox(
    mbox_path: Path,
    conn: sqlite3.Connection,
    max_body_chars: int,
    show_progress: bool,
) -> tuple[int, IndexStats]:
    current_header_lines: list[bytes] = []
    current_headers: dict[str, str] = {}
    current_body_parts: list[str] = []
    current_body_chars = 0
    in_headers = False
    have_message = False
    message_index = 0
    message_start_offset = 0
    attachment_count = 0
    pending_disposition = b""
    pending_attachment_counted = False
    pending_qp_line = b""
    stats = IndexStats()

    conn.execute("BEGIN")
    with mbox_path.open("rb") as handle:
        while True:
            line_offset = handle.tell()
            raw_line = handle.readline()
            if not raw_line:
                break
            if raw_line.startswith(b"From "):
                if have_message:
                    message_index += 1
                    current_headers["__attachment_count__"] = str(attachment_count)
                    finalize_index_message(
                        conn,
                        message_index,
                        str(mbox_path),
                        message_start_offset,
                        current_headers,
                        current_body_parts,
                        stats,
                    )
                    if message_index % COMMIT_EVERY == 0:
                        conn.commit()
                        conn.execute("BEGIN")
                    if show_progress and message_index % PROGRESS_EVERY == 0:
                        print(
                            f"Indexed {message_index} messages...",
                            file=sys.stderr,
                            flush=True,
                        )

                current_header_lines = []
                current_headers = {}
                current_body_parts = []
                current_body_chars = 0
                message_start_offset = line_offset
                attachment_count = 0
                pending_disposition = b""
                pending_attachment_counted = False
                pending_qp_line = b""
                in_headers = True
                have_message = True
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

            if current_body_chars >= max_body_chars:
                continue

            candidates, pending_qp_line = consume_body_line(raw_line, pending_qp_line)
            text = choose_text_candidate(candidates)
            if not text:
                continue
            remaining = max_body_chars - current_body_chars
            clipped = text[:remaining]
            if clipped:
                current_body_parts.append(clipped)
                current_body_chars += len(clipped)

    if have_message:
        message_index += 1
        current_headers["__attachment_count__"] = str(attachment_count)
        finalize_index_message(
            conn,
            message_index,
            str(mbox_path),
            message_start_offset,
            current_headers,
            current_body_parts,
            stats,
        )
    conn.commit()
    return message_index, stats


def write_index_metadata(
    conn: sqlite3.Connection,
    mbox_path: Path,
    message_count: int,
    max_body_chars: int,
    stats: IndexStats,
) -> None:
    metadata = {
        "source_path": str(mbox_path),
        "source_size_bytes": str(mbox_path.stat().st_size),
        "indexed_at_utc": datetime.now(UTC).isoformat(),
        "message_count": str(message_count),
        "max_body_chars": str(max_body_chars),
        "multipart_messages": str(stats.multipart_messages),
        "attachment_messages": str(stats.attachment_messages),
        "attachment_files": str(stats.attachment_files),
    }
    conn.executemany(
        "INSERT INTO metadata(key, value) VALUES (?, ?)",
        metadata.items(),
    )


def handle_info(args: argparse.Namespace) -> None:
    path = Path(args.input_file)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    if detect_sqlite(path):
        (
            stats,
            sender_counts,
            recipient_counts,
            subject_counts,
            has_mime_stats,
        ) = scan_sqlite_info(path)
    else:
        stats, sender_counts, recipient_counts, subject_counts = scan_mbox_info(
            path, show_progress=not args.no_progress
        )
        has_mime_stats = True

    print_info(
        path,
        stats,
        sender_counts,
        recipient_counts,
        subject_counts,
        args.top,
        args.timezone,
        has_mime_stats,
    )


def handle_search(args: argparse.Namespace) -> None:
    if not args.terms and not any(
        [
            args.from_filter,
            args.to_filter,
            args.subject_filter,
            args.label_filter,
            args.after,
            args.before,
        ]
    ):
        raise SystemExit("Provide at least one search term or filter.")

    path = Path(args.input_file)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    if detect_sqlite(path):
        if args.headers_only:
            raise SystemExit("--headers-only is only supported for raw .mbox input.")
        if not args.no_progress:
            print("Using SQLite index.", file=sys.stderr)
        results = search_sqlite(args)
    else:
        results = search_mbox(args)
    print_results(results)


def handle_index(args: argparse.Namespace) -> None:
    mbox_path = Path(args.mbox_file)
    index_path = Path(args.index_file)
    if not mbox_path.exists():
        raise SystemExit(f"File not found: {mbox_path}")

    conn = open_database(index_path, force=args.force)
    try:
        message_count, stats = index_mbox(
            mbox_path,
            conn,
            max_body_chars=args.max_body_chars,
            show_progress=not args.no_progress,
        )
        write_index_metadata(conn, mbox_path, message_count, args.max_body_chars, stats)
        conn.commit()
    finally:
        conn.close()

    print(f"Index: {index_path}")
    print(f"Source: {mbox_path}")
    print(f"Messages indexed: {message_count}")
    print(f"Max body chars per message: {args.max_body_chars}")


def handle_show(args: argparse.Namespace) -> None:
    input_path = Path(args.input_file)
    if not input_path.exists():
        raise SystemExit(f"File not found: {input_path}")

    source_path = input_path
    source_offset: int | None = None
    if detect_sqlite(input_path):
        metadata = load_sqlite_metadata(input_path)
        source_value = metadata.get("source_path")
        if not source_value:
            raise SystemExit("This SQLite index does not include source_path metadata.")
        source_path = Path(source_value)
        if not source_path.exists():
            raise SystemExit(f"Source mbox from index metadata not found: {source_path}")
        conn = sqlite3.connect(input_path)
        try:
            try:
                row = conn.execute(
                    "SELECT source_offset FROM messages WHERE message_index = ?",
                    (args.message_index,),
                ).fetchone()
            except sqlite3.OperationalError:
                row = ()
        finally:
            conn.close()
        if row is None:
            raise SystemExit(f"Message index not found in index: {args.message_index}")
        if row:
            source_offset = row[0]

    if source_offset is not None:
        headers, body_text = load_message_from_offset(source_path, int(source_offset))
    else:
        headers, body_text = load_message_from_mbox(
            source_path, args.message_index, show_progress=not args.no_progress
        )
    print(format_full_message(headers, body_text, args.timezone))


def main() -> None:
    args = parse_args()
    if args.command == "info":
        handle_info(args)
    elif args.command == "search":
        handle_search(args)
    elif args.command == "index":
        handle_index(args)
    elif args.command == "show":
        handle_show(args)
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
