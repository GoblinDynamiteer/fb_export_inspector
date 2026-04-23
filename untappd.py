#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ANSI_RESET = "\033[0m"
ANSI_HIGHLIGHT = "\033[1;30;43m"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class SearchResult:
    index: int
    checkin_id: int
    created_at: str
    created_dt: datetime
    beer_name: str
    brewery_name: str
    beer_type: str
    venue_name: str
    rating: str
    rating_value: float | None
    snippet: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect an Untappd export with stats, search, and detailed check-in view."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    info_parser = subparsers.add_parser("info", help="Show summary stats for an Untappd export.")
    info_parser.add_argument("input_file", help="Path to the Untappd JSON export.")
    info_parser.add_argument("--top", type=int, default=10, help="Top-N lists, default: 10.")
    info_parser.add_argument(
        "--min-ratings-for-top-rated",
        type=int,
        default=3,
        help="Minimum number of rated check-ins for a beer to appear in top-rated beers, default: 3.",
    )
    info_parser.add_argument(
        "--beer-count",
        dest="beer_count",
        help="Show focused counts and breakdowns for one beer name.",
    )
    info_parser.add_argument(
        "--venue-count",
        dest="venue_count",
        help="Show focused counts and breakdowns for one venue name.",
    )

    search_parser = subparsers.add_parser("search", help="Search Untappd check-ins.")
    search_parser.add_argument("input_file", help="Path to the Untappd JSON export.")
    search_parser.add_argument("terms", nargs="*", help="Search terms.")
    search_parser.add_argument("--beer", dest="beer_filter", help="Filter by beer name text.")
    search_parser.add_argument("--brewery", dest="brewery_filter", help="Filter by brewery text.")
    search_parser.add_argument("--type", dest="type_filter", help="Filter by beer type/style text.")
    search_parser.add_argument("--venue", dest="venue_filter", help="Filter by venue text.")
    search_parser.add_argument("--country", dest="country_filter", help="Filter by brewery or venue country text.")
    search_parser.add_argument("--after", help="Only include check-ins on or after YYYY-MM-DD.")
    search_parser.add_argument("--from", dest="from_date", help="Alias for --after.")
    search_parser.add_argument("--before", help="Only include check-ins before YYYY-MM-DD.")
    search_parser.add_argument("--min-rating", type=float, help="Only include ratings >= this value.")
    search_parser.add_argument("--max-rating", type=float, help="Only include ratings <= this value.")
    search_parser.add_argument("--any", action="store_true", help="Match any term instead of all terms.")
    search_parser.add_argument("--limit", type=int, default=20, help="Maximum results, default: 20.")
    search_parser.add_argument(
        "--sort",
        choices=["date-desc", "date-asc", "rating-desc", "rating-asc", "beer", "brewery"],
        default="date-desc",
        help="Sort order for results, default: date-desc.",
    )
    search_parser.add_argument("--no-ansi", action="store_true", help="Disable ANSI highlights.")

    show_parser = subparsers.add_parser("show", help="Show one full check-in.")
    show_parser.add_argument("input_file", help="Path to the Untappd JSON export.")
    show_parser.add_argument(
        "identifier",
        help="Check-in number from search results or a raw checkin_id.",
    )

    return parser.parse_args()


def load_export(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"File not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Expected the Untappd export to be a JSON array.")
    return data


def parse_created_at(value: str) -> datetime:
    return datetime.strptime(value, DATE_FORMAT).replace(tzinfo=ZoneInfo("Europe/Stockholm"))


def parse_date_filter(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=ZoneInfo("Europe/Stockholm"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format: YYYY-MM-DD."
        ) from exc


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_rating(entry: dict[str, Any]) -> float | None:
    value = entry.get("rating_score")
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def display_rating(entry: dict[str, Any]) -> str:
    rating = parse_rating(entry)
    if rating is None:
        return "-"
    return f"{rating:.2f}".rstrip("0").rstrip(".")


def use_ansi(args: argparse.Namespace) -> bool:
    return sys.stdout.isatty() and not getattr(args, "no_ansi", False)


def highlight_text(text: str, terms: list[str], ansi_enabled: bool) -> str:
    if not text:
        return text

    matches: list[tuple[int, int]] = []
    lowered = text.lower()
    for term in sorted({term for term in terms if term}, key=len, reverse=True):
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


def shorten(text: str, limit: int = 220) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def entry_search_fields(entry: dict[str, Any]) -> list[tuple[str, str]]:
    return [
        ("Beer", normalize_text(entry.get("beer_name"))),
        ("Brewery", normalize_text(entry.get("brewery_name"))),
        ("Type", normalize_text(entry.get("beer_type"))),
        ("Comment", normalize_text(entry.get("comment"))),
        ("Venue", normalize_text(entry.get("venue_name"))),
        ("Purchase venue", normalize_text(entry.get("purchase_venue"))),
        ("Flavor", normalize_text(entry.get("flavor_profiles"))),
        ("Serving", normalize_text(entry.get("serving_type"))),
        ("Brewery country", normalize_text(entry.get("brewery_country"))),
        ("Venue country", normalize_text(entry.get("venue_country"))),
        ("Tagged friends", normalize_text(entry.get("tagged_friends"))),
    ]


def entry_matches(entry: dict[str, Any], args: argparse.Namespace) -> tuple[bool, str]:
    beer = normalize_text(entry.get("beer_name"))
    brewery = normalize_text(entry.get("brewery_name"))
    beer_type = normalize_text(entry.get("beer_type"))
    venue = normalize_text(entry.get("venue_name"))
    purchase_venue = normalize_text(entry.get("purchase_venue"))
    brewery_country = normalize_text(entry.get("brewery_country"))
    venue_country = normalize_text(entry.get("venue_country"))
    comment = normalize_text(entry.get("comment"))

    if args.beer_filter and args.beer_filter.lower() not in beer.lower():
        return False, ""
    if args.brewery_filter and args.brewery_filter.lower() not in brewery.lower():
        return False, ""
    if args.type_filter and args.type_filter.lower() not in beer_type.lower():
        return False, ""
    if args.venue_filter:
        venue_blob = " ".join([venue.lower(), purchase_venue.lower()])
        if args.venue_filter.lower() not in venue_blob:
            return False, ""
    if args.country_filter:
        country_blob = " ".join([brewery_country.lower(), venue_country.lower()])
        if args.country_filter.lower() not in country_blob:
            return False, ""

    after_dt = parse_date_filter(args.from_date or args.after)
    before_dt = parse_date_filter(args.before)
    created_dt = parse_created_at(normalize_text(entry.get("created_at")))
    if after_dt and created_dt < after_dt:
        return False, ""
    if before_dt and created_dt >= before_dt:
        return False, ""

    rating = parse_rating(entry)
    if args.min_rating is not None and (rating is None or rating < args.min_rating):
        return False, ""
    if args.max_rating is not None and (rating is None or rating > args.max_rating):
        return False, ""

    query_terms = [term.lower() for term in args.terms]
    if not query_terms:
        snippet_source = comment or beer or brewery or beer_type
        return True, shorten(snippet_source)

    fields = entry_search_fields(entry)
    matches: set[str] = set()
    snippet = ""
    for name, value in fields:
        if not value:
            continue
        lowered = value.lower()
        hit_terms = [term for term in query_terms if term in lowered]
        if not hit_terms:
            continue
        matches.update(hit_terms)
        if not snippet:
            snippet = f"{name}: {shorten(value)}"
        if args.any:
            break

    matched = bool(matches) if args.any else len(matches) == len(query_terms)
    if not matched:
        return False, ""
    return True, snippet


def build_result(
    index: int,
    entry: dict[str, Any],
    snippet: str,
    args: argparse.Namespace,
) -> SearchResult:
    created_dt = parse_created_at(normalize_text(entry.get("created_at")))
    rating_value = parse_rating(entry)
    return SearchResult(
        index=index,
        checkin_id=int(entry["checkin_id"]),
        created_at=normalize_text(entry.get("created_at")),
        created_dt=created_dt,
        beer_name=normalize_text(entry.get("beer_name")),
        brewery_name=normalize_text(entry.get("brewery_name")),
        beer_type=normalize_text(entry.get("beer_type")),
        venue_name=normalize_text(entry.get("venue_name")) or "-",
        rating=display_rating(entry),
        rating_value=rating_value,
        snippet=highlight_text(snippet, [term.lower() for term in args.terms], use_ansi(args)),
    )


def sort_results(results: list[SearchResult], sort_key: str) -> list[SearchResult]:
    if sort_key == "date-asc":
        return sorted(results, key=lambda item: (item.created_dt, item.index))
    if sort_key == "date-desc":
        return sorted(results, key=lambda item: (item.created_dt, item.index), reverse=True)
    if sort_key == "rating-desc":
        return sorted(
            results,
            key=lambda item: (
                item.rating_value is None,
                -(item.rating_value or -1),
                item.created_dt,
            ),
        )
    if sort_key == "rating-asc":
        return sorted(
            results,
            key=lambda item: (
                item.rating_value is None,
                item.rating_value if item.rating_value is not None else 999,
                item.created_dt,
            ),
        )
    if sort_key == "beer":
        return sorted(results, key=lambda item: (item.beer_name.lower(), item.created_dt))
    if sort_key == "brewery":
        return sorted(results, key=lambda item: (item.brewery_name.lower(), item.created_dt))
    return results


def handle_info(args: argparse.Namespace) -> None:
    data = load_export(Path(args.input_file))
    if not data:
        raise SystemExit("The Untappd export is empty.")

    if args.beer_count and args.venue_count:
        raise SystemExit("Use only one of --beer-count or --venue-count at a time.")

    if args.beer_count:
        target = args.beer_count.lower()
        filtered = [
            entry
            for entry in data
            if target in normalize_text(entry.get("beer_name")).lower()
        ]
        if not filtered:
            raise SystemExit(f"No beer matches found for: {args.beer_count}")
        matched_names = Counter(normalize_text(entry.get("beer_name")) or "(unknown)" for entry in filtered)
        if len(matched_names) > 1:
            print_multi_focus_info(
                filtered,
                focus_label="Beer",
                top=args.top,
                group_key="beer_name",
            )
        else:
            print_focus_info(
                filtered,
                focus_label="Beer",
                focus_value=render_focus_value(args.beer_count, matched_names),
                top=args.top,
                matched_names=matched_names,
            )
        return

    if args.venue_count:
        target = args.venue_count.lower()
        filtered = [
            entry
            for entry in data
            if target in " ".join(
                [
                    normalize_text(entry.get("venue_name")).lower(),
                    normalize_text(entry.get("purchase_venue")).lower(),
                ]
            )
        ]
        if not filtered:
            raise SystemExit(f"No venue matches found for: {args.venue_count}")
        matched_names = Counter(
            normalize_text(entry.get("venue_name")) or normalize_text(entry.get("purchase_venue")) or "(unknown)"
            for entry in filtered
        )
        if len(matched_names) > 1:
            print_multi_focus_info(
                filtered,
                focus_label="Venue",
                top=args.top,
                group_key="venue_name",
            )
        else:
            print_focus_info(
                filtered,
                focus_label="Venue",
                focus_value=render_focus_value(args.venue_count, matched_names),
                top=args.top,
                matched_names=matched_names,
            )
        return

    dates = [parse_created_at(normalize_text(entry.get("created_at"))) for entry in data]
    ratings = [rating for entry in data if (rating := parse_rating(entry)) is not None]

    unique_beers = {normalize_text(entry.get("bid")) or normalize_text(entry.get("beer_name")) for entry in data}
    unique_breweries = {normalize_text(entry.get("brewery_id")) or normalize_text(entry.get("brewery_name")) for entry in data}
    unique_venues = {normalize_text(entry.get("venue_name")) for entry in data if normalize_text(entry.get("venue_name"))}

    styles = Counter(normalize_text(entry.get("beer_type")) or "(unknown)" for entry in data)
    breweries = Counter(normalize_text(entry.get("brewery_name")) or "(unknown)" for entry in data)
    beers = Counter(normalize_text(entry.get("beer_name")) or "(unknown)" for entry in data)
    venues = Counter(normalize_text(entry.get("venue_name")) for entry in data if normalize_text(entry.get("venue_name")))
    countries = Counter(normalize_text(entry.get("brewery_country")) or "(unknown)" for entry in data)
    serving_types = Counter(normalize_text(entry.get("serving_type")) or "(unknown)" for entry in data)
    years = Counter(parse_created_at(normalize_text(entry.get("created_at"))).year for entry in data)
    rated_styles = Counter(
        normalize_text(entry.get("beer_type")) or "(unknown)"
        for entry in data
        if parse_rating(entry) is not None
    )
    year_country_pairs = Counter(
        (
            parse_created_at(normalize_text(entry.get("created_at"))).year,
            normalize_text(entry.get("brewery_country")) or "(unknown)",
        )
        for entry in data
    )
    avg_rating_by_year: dict[int, float] = {}
    for year in sorted(years):
        year_ratings = [
            rating
            for entry in data
            if parse_created_at(normalize_text(entry.get("created_at"))).year == year
            and (rating := parse_rating(entry)) is not None
        ]
        if year_ratings:
            avg_rating_by_year[year] = sum(year_ratings) / len(year_ratings)

    print(f"File: {args.input_file}")
    print(f"Check-ins: {len(data)}")
    print(f"Date range: {min(dates).strftime(DATE_FORMAT)} to {max(dates).strftime(DATE_FORMAT)}")
    print(f"Unique beers: {len(unique_beers)}")
    print(f"Unique breweries: {len(unique_breweries)}")
    print(f"Unique venues: {len(unique_venues)}")
    if ratings:
        print(f"Rated check-ins: {len(ratings)}")
        print(f"Average rating: {sum(ratings) / len(ratings):.2f}")
        print(f"Highest rating: {max(ratings):.2f}")
        print(f"Lowest rating: {min(ratings):.2f}")
    else:
        print("Rated check-ins: 0")
        print("Average rating: n/a")

    print()
    print(f"Top breweries ({args.top}):")
    for name, count in breweries.most_common(args.top):
        print(f"  {count:>6}  {name}")

    print()
    print(f"Top styles ({args.top}):")
    for name, count in styles.most_common(args.top):
        print(f"  {count:>6}  {name}")

    print()
    print(f"Top venues ({args.top}):")
    for name, count in venues.most_common(args.top):
        print(f"  {count:>6}  {name}")

    print()
    print(f"Top brewery countries ({args.top}):")
    for name, count in countries.most_common(args.top):
        print(f"  {count:>6}  {name}")

    print()
    print(f"Top serving types ({args.top}):")
    for name, count in serving_types.most_common(args.top):
        print(f"  {count:>6}  {name}")

    print()
    print(f"Top years ({args.top}):")
    for year, count in years.most_common(args.top):
        avg_text = ""
        if year in avg_rating_by_year:
            avg_text = f"  avg_rating={avg_rating_by_year[year]:.2f}"
        print(f"  {count:>6}  {year}{avg_text}")

    print()
    print(f"Top checked-in beers ({args.top}):")
    for name, count in beers.most_common(args.top):
        beer_ratings = [
            rating
            for entry in data
            if (normalize_text(entry.get("beer_name")) or "(unknown)") == name
            and (rating := parse_rating(entry)) is not None
        ]
        avg_text = f"{sum(beer_ratings) / len(beer_ratings):.2f}" if beer_ratings else "n/a"
        print(f"  {count:>6}  {name}  avg_rating={avg_text}")

    top_rated_candidates: list[tuple[str, float, int]] = []
    for name, count in beers.items():
        beer_ratings = [
            rating
            for entry in data
            if (normalize_text(entry.get("beer_name")) or "(unknown)") == name
            and (rating := parse_rating(entry)) is not None
        ]
        if len(beer_ratings) < args.min_ratings_for_top_rated:
            continue
        avg_rating = sum(beer_ratings) / len(beer_ratings)
        top_rated_candidates.append((name, avg_rating, len(beer_ratings)))

    top_rated_candidates.sort(key=lambda item: (-item[1], -item[2], item[0].lower()))
    print()
    print(
        f"Top rated beers ({args.top}, min {args.min_ratings_for_top_rated} ratings):"
    )
    for name, avg_rating, rating_count in top_rated_candidates[: args.top]:
        print(f"  {avg_rating:>6.2f}  {name}  ratings={rating_count}")

    print()
    print(f"Top rated styles by volume ({args.top}):")
    for name, count in rated_styles.most_common(args.top):
        style_ratings = [
            rating
            for entry in data
            if (normalize_text(entry.get('beer_type')) or "(unknown)") == name
            and (rating := parse_rating(entry)) is not None
        ]
        avg_text = f"{sum(style_ratings) / len(style_ratings):.2f}" if style_ratings else "n/a"
        print(f"  {count:>6}  {name}  avg_rating={avg_text}")

    print()
    print(f"Top year/country pairs ({args.top}):")
    for (year, country), count in year_country_pairs.most_common(args.top):
        print(f"  {count:>6}  {year} / {country}")


def print_focus_info(
    filtered: list[dict[str, Any]],
    focus_label: str,
    focus_value: str,
    top: int,
    matched_names: Counter[str] | None = None,
) -> None:
    dates = [parse_created_at(normalize_text(entry.get("created_at"))) for entry in filtered]
    ratings = [rating for entry in filtered if (rating := parse_rating(entry)) is not None]
    breweries = Counter(normalize_text(entry.get("brewery_name")) or "(unknown)" for entry in filtered)
    beers = Counter(normalize_text(entry.get("beer_name")) or "(unknown)" for entry in filtered)
    venues = Counter(
        normalize_text(entry.get("venue_name")) or normalize_text(entry.get("purchase_venue")) or "(unknown)"
        for entry in filtered
    )
    serving_types = Counter(normalize_text(entry.get("serving_type")) or "(unknown)" for entry in filtered)
    years = Counter(parse_created_at(normalize_text(entry.get("created_at"))).year for entry in filtered)
    comments = sum(1 for entry in filtered if normalize_text(entry.get("comment")))
    photos = sum(1 for entry in filtered if normalize_text(entry.get("photo_url")))
    tagged = sum(1 for entry in filtered if normalize_text(entry.get("tagged_friends")))

    print(f"{focus_label}: {focus_value}")
    print(f"Check-ins: {len(filtered)}")
    print(f"First check-in: {min(dates).strftime(DATE_FORMAT)}")
    print(f"Last check-in: {max(dates).strftime(DATE_FORMAT)}")
    if ratings:
        print(f"Average rating: {sum(ratings) / len(ratings):.2f}")
        print(f"Highest rating: {max(ratings):.2f}")
        print(f"Lowest rating: {min(ratings):.2f}")
    else:
        print("Average rating: n/a")
    print(f"With comments: {comments}")
    print(f"With photos: {photos}")
    print(f"With tagged friends: {tagged}")

    if matched_names and len(matched_names) > 1:
        print()
        print(f"Matched {focus_label.lower()} names ({min(top, len(matched_names))}):")
        for name, count in matched_names.most_common(top):
            print(f"  {count:>6}  {name}")

    if focus_label == "Beer":
        first = filtered[0]
        print(f"Brewery: {normalize_text(first.get('brewery_name')) or '-'}")
        print(f"Type: {normalize_text(first.get('beer_type')) or '-'}")
        print(f"ABV: {format_float(first.get('beer_abv'))}")
        print(f"Beer ID: {format_float(first.get('bid'))}")
        print(f"Distinct venues: {len({normalize_text(entry.get('venue_name')) for entry in filtered if normalize_text(entry.get('venue_name'))})}")
    elif focus_label == "Venue":
        print(f"Distinct beers: {len({normalize_text(entry.get('bid')) or normalize_text(entry.get('beer_name')) for entry in filtered})}")
        print(f"Distinct breweries: {len({normalize_text(entry.get('brewery_id')) or normalize_text(entry.get('brewery_name')) for entry in filtered})}")

    print()
    print(f"Year breakdown ({top}):")
    for year, count in years.most_common(top):
        year_ratings = [
            rating
            for entry in filtered
            if parse_created_at(normalize_text(entry.get("created_at"))).year == year
            and (rating := parse_rating(entry)) is not None
        ]
        avg_text = f"  avg_rating={sum(year_ratings) / len(year_ratings):.2f}" if year_ratings else ""
        print(f"  {count:>6}  {year}{avg_text}")

    if focus_label == "Venue":
        print()
        print(f"Top beers there ({top}):")
        for name, count in beers.most_common(top):
            print(f"  {count:>6}  {name}")

        print()
        print(f"Top breweries there ({top}):")
        for name, count in breweries.most_common(top):
            print(f"  {count:>6}  {name}")

    if focus_label == "Beer":
        print()
        print(f"Top venues for this beer ({top}):")
        for name, count in venues.most_common(top):
            print(f"  {count:>6}  {name}")

    print()
    print(f"Serving types ({top}):")
    for name, count in serving_types.most_common(top):
        print(f"  {count:>6}  {name}")


def render_focus_value(query: str, matched_names: Counter[str]) -> str:
    if not matched_names:
        return query
    if len(matched_names) == 1:
        return next(iter(matched_names))
    top_name, top_count = matched_names.most_common(1)[0]
    if top_count == sum(matched_names.values()):
        return top_name
    return f"{query}  ({len(matched_names)} matches; top: {top_name})"


def print_multi_focus_info(
    filtered: list[dict[str, Any]],
    focus_label: str,
    top: int,
    group_key: str,
) -> None:
    if focus_label == "Beer":
        def group_name(entry: dict[str, Any]) -> str:
            return normalize_text(entry.get("beer_name")) or "(unknown)"
    else:
        def group_name(entry: dict[str, Any]) -> str:
            return (
                normalize_text(entry.get("venue_name"))
                or normalize_text(entry.get("purchase_venue"))
                or "(unknown)"
            )

    groups: dict[str, list[dict[str, Any]]] = {}
    for entry in filtered:
        groups.setdefault(group_name(entry), []).append(entry)

    print(f"{focus_label}: multiple matches")
    print(f"Matched {focus_label.lower()} names: {len(groups)}")
    print(f"Showing top {min(top, len(groups))} by check-in count")
    print()

    ranked_groups = sorted(
        groups.items(),
        key=lambda item: (-len(item[1]), item[0].lower()),
    )[:top]

    for idx, (name, entries) in enumerate(ranked_groups, start=1):
        dates = [parse_created_at(normalize_text(entry.get("created_at"))) for entry in entries]
        ratings = [rating for entry in entries if (rating := parse_rating(entry)) is not None]
        comments = sum(1 for entry in entries if normalize_text(entry.get("comment")))
        photos = sum(1 for entry in entries if normalize_text(entry.get("photo_url")))
        tagged = sum(1 for entry in entries if normalize_text(entry.get("tagged_friends")))
        serving_types = Counter(normalize_text(entry.get("serving_type")) or "(unknown)" for entry in entries)

        print(f"{idx}. {name}")
        print(f"   Check-ins: {len(entries)}")
        print(f"   First: {min(dates).strftime(DATE_FORMAT)}")
        print(f"   Last: {max(dates).strftime(DATE_FORMAT)}")
        if ratings:
            print(f"   Avg rating: {sum(ratings) / len(ratings):.2f}")
        else:
            print("   Avg rating: n/a")
        print(f"   With comments: {comments}")
        print(f"   With photos: {photos}")
        print(f"   With tagged friends: {tagged}")

        first = entries[0]
        if focus_label == "Beer":
            print(f"   Brewery: {normalize_text(first.get('brewery_name')) or '-'}")
            print(f"   Type: {normalize_text(first.get('beer_type')) or '-'}")
            print(f"   ABV: {format_float(first.get('beer_abv'))}")
            venues = Counter(
                normalize_text(entry.get("venue_name"))
                or normalize_text(entry.get("purchase_venue"))
                or "(unknown)"
                for entry in entries
            )
            print("   Top venues:")
            for venue_name, count in venues.most_common(3):
                print(f"     {count:>4}  {venue_name}")
        else:
            beers = Counter(normalize_text(entry.get("beer_name")) or "(unknown)" for entry in entries)
            breweries = Counter(normalize_text(entry.get("brewery_name")) or "(unknown)" for entry in entries)
            print(f"   Distinct beers: {len(beers)}")
            print(f"   Distinct breweries: {len(breweries)}")
            print("   Top beers:")
            for beer_name, count in beers.most_common(3):
                print(f"     {count:>4}  {beer_name}")

        print("   Serving types:")
        for serving_name, count in serving_types.most_common(3):
            print(f"     {count:>4}  {serving_name}")
        print()


def handle_search(args: argparse.Namespace) -> None:
    if not args.terms and not any(
        [
            args.beer_filter,
            args.brewery_filter,
            args.type_filter,
            args.venue_filter,
            args.country_filter,
            args.after,
            args.before,
            args.min_rating is not None,
            args.max_rating is not None,
        ]
    ):
        raise SystemExit("Provide at least one search term or filter.")

    data = load_export(Path(args.input_file))
    results: list[SearchResult] = []
    for index, entry in enumerate(data, start=1):
        matched, snippet = entry_matches(entry, args)
        if not matched:
            continue
        results.append(build_result(index, entry, snippet, args))

    results = sort_results(results, args.sort)[: args.limit]

    if not results:
        print("No matches found.")
        return

    for result in results:
        print(f"{result.index}. {result.created_at}  rating={result.rating}")
        print(f"   Beer: {result.beer_name}")
        print(f"   Brewery: {result.brewery_name}")
        print(f"   Type: {result.beer_type}")
        if result.venue_name != "-":
            print(f"   Venue: {result.venue_name}")
        print(f"   Check-in ID: {result.checkin_id}")
        print(f"   Snippet: {result.snippet}")
        print()


def format_float(value: Any) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.4f}".rstrip("0").rstrip(".")
        return "-"
    return str(value)


def handle_show(args: argparse.Namespace) -> None:
    data = load_export(Path(args.input_file))
    identifier = args.identifier.strip()

    entry: dict[str, Any] | None = None
    if identifier.isdigit():
        number = int(identifier)
        if 1 <= number <= len(data):
            entry = data[number - 1]
        if entry is None:
            for candidate in data:
                if int(candidate.get("checkin_id", -1)) == number:
                    entry = candidate
                    break
    if entry is None:
        raise SystemExit(f"Check-in not found: {identifier}")

    ordered_fields = [
        ("Beer", "beer_name"),
        ("Brewery", "brewery_name"),
        ("Type", "beer_type"),
        ("ABV", "beer_abv"),
        ("IBU", "beer_ibu"),
        ("Your rating", "rating_score"),
        ("Global rating", "global_rating_score"),
        ("Weighted global rating", "global_weighted_rating_score"),
        ("Created at", "created_at"),
        ("Venue", "venue_name"),
        ("Venue city", "venue_city"),
        ("Venue state", "venue_state"),
        ("Venue country", "venue_country"),
        ("Venue lat", "venue_lat"),
        ("Venue lng", "venue_lng"),
        ("Brewery city", "brewery_city"),
        ("Brewery state", "brewery_state"),
        ("Brewery country", "brewery_country"),
        ("Serving type", "serving_type"),
        ("Purchase venue", "purchase_venue"),
        ("Flavor profiles", "flavor_profiles"),
        ("Tagged friends", "tagged_friends"),
        ("Toasts", "total_toasts"),
        ("Comments", "total_comments"),
        ("Check-in ID", "checkin_id"),
        ("Beer ID", "bid"),
        ("Brewery ID", "brewery_id"),
        ("Check-in URL", "checkin_url"),
        ("Beer URL", "beer_url"),
        ("Brewery URL", "brewery_url"),
        ("Photo URL", "photo_url"),
    ]

    for label, key in ordered_fields:
        value = entry.get(key)
        if value in (None, ""):
            continue
        print(f"{label}: {format_float(value)}")

    comment = normalize_text(entry.get("comment"))
    if comment:
        print()
        print("Comment:")
        print(comment)


def main() -> None:
    args = parse_args()
    if args.command == "info":
        handle_info(args)
    elif args.command == "search":
        handle_search(args)
    elif args.command == "show":
        handle_show(args)
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
