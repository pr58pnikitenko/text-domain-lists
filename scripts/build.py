#!/usr/bin/env python3
"""
AWG Domain List Builder
Парсит ВСЕ теги из runetfreedom geosite.dat и генерирует по .txt файлу на каждый.
Никакого ручного списка сервисов — всё автоматически.
"""
import logging
import json
from pathlib import Path

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).parent.parent
OUTPUT = BASE / "output"
CONFIG = BASE / "config"

RUNETFREEDOM_API = (
    "https://api.github.com/repos/runetfreedom/russia-v2ray-rules-dat/releases/latest"
)
GOOGLE_IP_RANGES       = "https://www.gstatic.com/ipranges/goog.json"
GOOGLE_CLOUD_IP_RANGES = "https://www.gstatic.com/ipranges/cloud.json"
TELEGRAM_CIDR          = "https://core.telegram.org/resources/cidr.txt"

# Теги, которые объединяются в ru-only список (не генерируют отдельный файл)
RU_ONLY_TAGS = {"RU-AVAILABLE-ONLY-INSIDE", "CATEGORY-GOV-RU"}

session = requests.Session()
session.headers["User-Agent"] = "awg-domain-builder/1.0"


def fetch_geosite_dat() -> bytes:
    """
    Резолвим прямой CDN-URL через GitHub API (минуем редирект, который ломает SSL).
    Если и прямой URL падает с SSLError — повторяем без верификации.
    """
    log.info("Resolving geosite.dat URL via GitHub API...")
    api_resp = session.get(RUNETFREEDOM_API, timeout=15)
    api_resp.raise_for_status()

    download_url: str | None = None
    for asset in api_resp.json().get("assets", []):
        if asset["name"] == "geosite.dat":
            download_url = asset["browser_download_url"]
            break

    if not download_url:
        raise RuntimeError("geosite.dat not found in latest release assets")

    log.info("Downloading %s", download_url)
    try:
        resp = session.get(download_url, timeout=90)
        resp.raise_for_status()
        return resp.content
    except requests.exceptions.SSLError as e:
        log.warning("SSL error on direct URL, retrying with verify=False: %s", e)
        resp = session.get(download_url, timeout=90, verify=False)
        resp.raise_for_status()
        return resp.content


# ─── Minimal Protobuf Parser ──────────────────────────────────────────────────

def _varint(data: bytes, pos: int) -> tuple[int, int]:
    n, shift = 0, 0
    while True:
        b = data[pos]; pos += 1
        n |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return n, pos


def _ld(data: bytes, pos: int) -> tuple[bytes, int]:
    length, pos = _varint(data, pos)
    return data[pos : pos + length], pos + length


def _parse_domain(data: bytes) -> tuple[int, str]:
    """Returns (type, value).  0=plain  1=regex  2=subdomain  3=full"""
    pos, dtype, value = 0, 0, ""
    while pos < len(data):
        try:
            tw, pos = _varint(data, pos)
        except IndexError:
            break
        field, wire = tw >> 3, tw & 7
        if wire == 0:
            val, pos = _varint(data, pos)
            if field == 1:
                dtype = val
        elif wire == 2:
            chunk, pos = _ld(data, pos)
            if field == 2:
                value = chunk.decode("utf-8", errors="ignore")
        else:
            break
    return dtype, value


def _parse_geosite_entry(data: bytes) -> tuple[str, list[str]]:
    pos, code, domains = 0, "", []
    while pos < len(data):
        try:
            tw, pos = _varint(data, pos)
        except IndexError:
            break
        field, wire = tw >> 3, tw & 7
        if wire == 2:
            chunk, pos = _ld(data, pos)
            if field == 1:
                code = chunk.decode("utf-8", errors="ignore").upper()
            elif field == 2:
                dtype, value = _parse_domain(chunk)
                if value and dtype != 1:  # skip regex
                    domains.append(value)
        elif wire == 0:
            _, pos = _varint(data, pos)
        else:
            break
    return code, domains


def parse_geosite_dat(data: bytes) -> dict[str, list[str]]:
    """Parse ALL tags from geosite.dat → {TAG: [domains]}"""
    result: dict[str, list[str]] = {}
    pos = 0
    while pos < len(data):
        try:
            tw, pos = _varint(data, pos)
        except IndexError:
            break
        field, wire = tw >> 3, tw & 7
        if wire == 2:
            chunk, pos = _ld(data, pos)
            if field == 1:
                code, domains = _parse_geosite_entry(chunk)
                if code:
                    result[code] = domains
        elif wire == 0:
            _, pos = _varint(data, pos)
    return result


# ─── IP Fetchers ──────────────────────────────────────────────────────────────

def fetch_google_ips() -> list[str]:
    ips: list[str] = []
    for url in [GOOGLE_IP_RANGES, GOOGLE_CLOUD_IP_RANGES]:
        try:
            for prefix in session.get(url, timeout=15).json().get("prefixes", []):
                ip = prefix.get("ipv4Prefix") or prefix.get("ipv6Prefix", "")
                if ip:
                    ips.append(ip)
        except Exception as e:
            log.warning("Failed to fetch Google IPs from %s: %s", url, e)
    return ips


def fetch_telegram_ips() -> list[str]:
    try:
        r = session.get(TELEGRAM_CIDR, timeout=15)
        return [l.strip() for l in r.text.splitlines() if l.strip() and not l.startswith("#")]
    except Exception as e:
        log.warning("Failed to fetch Telegram IPs: %s", e)
        return []


def fetch_url_ips(url: str) -> list[str]:
    try:
        r = session.get(url, timeout=15)
        return [l.strip() for l in r.text.splitlines() if l.strip() and not l.startswith("#")]
    except Exception as e:
        log.warning("Failed to fetch IPs from %s: %s", url, e)
        return []


_ip_fetchers = {
    "google":   fetch_google_ips,
    "telegram": fetch_telegram_ips,
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def normalize(domains: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for d in domains:
        d = d.strip().lower().lstrip(".")
        if d and "." in d and d not in seen:
            seen.add(d)
            out.append(d)
    return sorted(out)


def write_output(path: Path, domains: list[str], ips: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = domains + list(dict.fromkeys(ips or []))
    path.write_text("\n".join(entries) + "\n", encoding="utf-8")
    extra = f" + {len(ips)} IPs" if ips else ""
    log.info("  ✓ %-55s %d domains%s", str(path.relative_to(BASE)), len(domains), extra)


def load_yaml(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


# ─── Builders ─────────────────────────────────────────────────────────────────

def build_ru_only(geosite: dict[str, list[str]]) -> None:
    log.info("─ Building ru-only list")
    domains: list[str] = []

    for tag in RU_ONLY_TAGS:
        chunk = geosite.get(tag, [])
        log.info("  [%s] %d domains", tag, len(chunk))
        domains.extend(chunk)

    extra_file = CONFIG / "ru_extra.txt"
    if extra_file.exists():
        extras = [
            l.strip()
            for l in extra_file.read_text().splitlines()
            if l.strip() and not l.startswith("#")
        ]
        log.info("  [ru_extra.txt] %d domains", len(extras))
        domains.extend(extras)

    write_output(OUTPUT / "ru-only" / "domains.txt", normalize(domains))


def build_services(
    geosite: dict[str, list[str]],
    ip_sources: dict,
    extra_domains: dict,
) -> None:
    log.info("─ Building per-service lists (%d tags)", len(geosite) - len(RU_ONLY_TAGS))

    # Кэш IP — чтобы не делать несколько запросов к одному источнику
    ip_cache: dict[str, list[str]] = {}

    def get_ips(source_key: str, source_cfg) -> list[str]:
        if source_key not in ip_cache:
            if isinstance(source_cfg, str):
                # shorthand: тип фетчера
                fetcher = _ip_fetchers.get(source_cfg)
                ip_cache[source_key] = fetcher() if fetcher else []
            elif isinstance(source_cfg, dict) and source_cfg.get("url"):
                ip_cache[source_key] = fetch_url_ips(source_cfg["url"])
            else:
                ip_cache[source_key] = []
        return ip_cache[source_key]

    for tag, domains in geosite.items():
        if tag in RU_ONLY_TAGS:
            continue

        slug = tag.lower()  # YOUTUBE → youtube
        all_domains = list(domains)

        # Ручные добавки из extra_domains.yml
        extras = extra_domains.get(slug, []) or extra_domains.get(tag, [])
        if extras:
            log.info("  [%s] +%d extra domains", slug, len(extras))
            all_domains.extend(extras)

        # IP-адреса из ip_sources.yml
        ips: list[str] = []
        ip_cfg = ip_sources.get(slug) or ip_sources.get(tag)
        if ip_cfg:
            sources = ip_cfg if isinstance(ip_cfg, list) else [ip_cfg]
            for src in sources:
                src_key = src if isinstance(src, str) else src.get("url", str(src))
                chunk = get_ips(src_key, src)
                if chunk:
                    log.info("  [%s] +%d IPs from %s", slug, len(chunk), src_key)
                    ips.extend(chunk)

        write_output(
            OUTPUT / "services" / f"{slug}.txt",
            normalize(all_domains),
            ips or None,
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ip_sources   = load_yaml(CONFIG / "ip_sources.yml")
    extra_domains = load_yaml(CONFIG / "extra_domains.yml")

    log.info("Fetching runetfreedom geosite.dat...")
    try:
        geosite = parse_geosite_dat(fetch_geosite_dat())
        log.info("Parsed %d tags total", len(geosite))
    except Exception as e:
        log.error("Failed to fetch geosite.dat: %s", e)
        raise SystemExit(1)

    build_ru_only(geosite)
    build_services(geosite, ip_sources, extra_domains)

    log.info("Done! Output → %s/", OUTPUT.relative_to(BASE))


if __name__ == "__main__":
    main()
