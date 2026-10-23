"""Read and parse the CMS Provider Data Catalog metadata API.

The catalog exposes a single JSON endpoint listing every dataset with its
`modified` timestamp, `theme`, and one or more `distribution` entries (the
actual file download URLs). We turn that into a flat list of `Dataset` records,
one per CSV distribution, filtered to a theme.

Docs: https://data.cms.gov/provider-data/ (DKAN metastore API)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, List, Optional, Protocol

DEFAULT_METADATA_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)
DEFAULT_THEME = "Hospitals"


class _Response(Protocol):
    def raise_for_status(self) -> None: ...
    def json(self) -> object: ...


class _Getter(Protocol):
    def get(self, url: str, *, timeout: float = ...) -> _Response: ...


@dataclass(frozen=True)
class Dataset:
    """One downloadable CSV distribution of a catalog dataset."""

    identifier: str
    title: str
    modified: str
    download_url: str
    media_type: str = "text/csv"

    @property
    def key(self) -> str:
        """Stable manifest key: identifier + the URL's filename.

        CMS download URLs embed a hash and epoch in a directory
        (".../resources/<hash>_<epoch>/Name.csv") that changes every time the file
        is republished, so the full URL cannot be the key: an updated file would
        look brand new and stale entries would pile up. The trailing filename is
        stable, and a dataset can have several distributions, so the identifier
        alone is not unique either.
        """
        return f"{self.identifier}::{url_filename(self.download_url)}"


def url_filename(url: str) -> str:
    """The last path segment of a URL, without any query string."""
    return url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]


def fetch_datasets(
    session: _Getter,
    url: str = DEFAULT_METADATA_URL,
    theme: Optional[str] = DEFAULT_THEME,
    timeout: float = 180.0,  # the catalog endpoint is occasionally very slow (~90s seen)
) -> List[Dataset]:
    """Fetch the catalog and return the CSV datasets for `theme`.

    Theme matching ignores case and surrounding spaces, and a dataset tagged with
    several themes matches if any of them is `theme`. Pass `theme=None` to return
    CSV distributions for every theme.
    """
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    items = resp.json()
    if not isinstance(items, list):
        raise ValueError("metadata endpoint did not return a JSON array")
    return list(_parse_items(items, theme))


def _parse_items(items: list, theme: Optional[str]) -> Iterator[Dataset]:
    wanted = theme.strip().casefold() if theme is not None else None
    for item in items:
        if not isinstance(item, dict):
            continue
        themes = item.get("theme") or []
        if isinstance(themes, str):
            themes = [themes]
        if wanted is not None and wanted not in {str(t).strip().casefold() for t in themes}:
            continue
        identifier = str(item.get("identifier", ""))
        dataset_title = str(item.get("title", ""))
        modified = str(item.get("modified", ""))
        csv_dists = []
        for dist in item.get("distribution") or []:
            if not isinstance(dist, dict):
                continue
            url = dist.get("downloadURL")
            if not url:
                continue
            is_csv = (
                dist.get("mediaType") == "text/csv"
                or str(url).lower().split("?", 1)[0].endswith(".csv")
            )
            if is_csv:
                csv_dists.append((dist, str(url), str(dist.get("title") or dataset_title or identifier)))
        # Output files are named from the title, so two CSVs of one dataset that share
        # a title would write the same path. Tell them apart by their URL filename.
        titles = [t for _, _, t in csv_dists]
        for dist, url, title in csv_dists:
            if titles.count(title) > 1:
                title = f"{title} {url_filename(url).rsplit('.', 1)[0]}"
            yield Dataset(
                identifier=identifier,
                title=title,
                modified=modified,
                download_url=url,
                media_type=str(dist.get("mediaType", "text/csv")),
            )
