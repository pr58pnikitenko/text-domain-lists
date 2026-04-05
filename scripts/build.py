#!/usr/bin/env python3
"""
AWG Domain List Builder — fast edition

Оптимизации vs предыдущей версии:
  1. google-protobuf C extension вместо ручного байт-парсера (~30x быстрее)
  2. Параллельная загрузка: оба репо качаются одновременно,
     файлы внутри каждого — тоже параллельно (ThreadPoolExecutor)

Источники доменов:
  1. runetfreedom/russia-v2ray-rules-dat  — geosite.dat
  2. runetfreedom/russia-blocked-geosite  — geosite.dat + geosite-ru-only.dat
                                            + отдельные .txt (discord, google, …)
"""
import logging
import concurrent.futures
from pathlib import Path

import requests
import yaml
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool
from google.protobuf.message_factory import GetMessageClass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE   = Path(__file__).parent.parent
OUTPUT = BASE / "output"
CONFIG = BASE / "config"

# ─── URLs ─────────────────────────────────────────────────────────────────────

V2RAY_RULES_API = (
    "https://api.github.com/repos/runetfreedom/russia-v2ray-rules-dat/releases/latest"
)
BLOCKED_GEOSITE_API = (
    "https://api.github.com/repos/runetfreedom/russia-blocked-geosite/releases/latest"
)
GOOGLE_IP_RANGES       = "https://www.gstatic.com/ipranges/goog.json"
GOOGLE_CLOUD_IP_RANGES = "https://www.gstatic.com/ipranges/cloud.json"
TELEGRAM_CIDR          = "https://core.telegram.org/resources/cidr.txt"

# ─── Config ───────────────────────────────────────────────────────────────────

RU_ONLY_TAGS = {"RU-AVAILABLE-ONLY-INSIDE", "CATEGORY-GOV-RU"}

BLOCKED_TXT_ROUTING: dict[str, str | None] = {
    "antifilter-download-community.txt": "ru-only",
    "discord.txt":                       "discord",
    "google.txt":                        "google",
    "refilter.txt":                      "ru-only",
    "ru-blocked-all.txt":                "ru-only",
    "win-extra.txt":                     "win-extra",
    "win-update.txt":                    "win-update",
    "youtube.txt":                       "youtube",
}

# Параллелизм: сколько файлов качать одновременно внутри одного релиза
DOWNLOAD_WORKERS = 8

session = requests.Session()
session.headers["User-Agent"] = "awg-domain-builder/1.0"


# ─── Protobuf Setup (один раз при старте) ─────────────────────────────────────

def _build_geosite_class():
    """
    Регистрирует схему geosite.dat через google-protobuf.

    Схема (упрощённая, regex-атрибуты не нужны):
        message Domain      { int32  type         = 1; string value        = 2; }
        message GeoSite     { string country_code  = 1; repeated Domain domain = 2; }
        message GeoSiteList { repeated GeoSite entry = 1; }

    google-protobuf использует C extension → парсинг 5 MB файла занимает
    <1 сек вместо 30-60 сек на ручном байт-парсере.
    """
    FD = _descriptor.FieldDescriptor

    fdp        = descriptor_pb2.FileDescriptorProto()
    fdp.name   = "v2ray_geosite.proto"
    fdp.syntax = "proto3"

    # message Domain
    msg = fdp.message_type.add()
    msg.name = "Domain"
    f = msg.field.add(); f.name, f.number, f.type, f.label = "type",  1, FD.TYPE_INT32,   FD.LABEL_OPTIONAL
    f = msg.field.add(); f.name, f.number, f.type, f.label = "value", 2, FD.TYPE_STRING,  FD.LABEL_OPTIONAL

    # message GeoSite
    msg = fdp.message_type.add()
    msg.name = "GeoSite"
    f = msg.field.add(); f.name, f.number, f.type, f.label = "country_code", 1, FD.TYPE_STRING,  FD.LABEL_OPTIONAL
    f = msg.field.add(); f.name, f.number, f.type, f.label = "domain",       2, FD.TYPE_MESSAGE, FD.LABEL_REPEATED
    f.type_name = ".Domain"

    # message GeoSiteList
    msg = fdp.message_type.add()
    msg.name = "GeoSiteList"
    f = msg.field.add(); f.name, f.number, f.type, f.label = "entry", 1, FD.TYPE_MESSAGE, FD.LABEL_REPEATED
    f.type_name = ".GeoSite"

    pool    = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return GetMessageClass(pool.FindMessageTypeByName("GeoSiteList"))


_GeoSiteList = _build_geosite_class()


def parse_geosite_dat(data: bytes) -> dict[str, list[str]]:
    """
    Парсит geosite.dat → {TAG: [domains]}. Regex-домены пропускаются (type=1).
    geo.Clear() вызывается явно — protobuf C extension держит свою память
    отдельно от Python heap, gc.collect() её не видит.
    """
    geo = _GeoSiteList()
    geo.ParseFromString(data)
    result = {
        entry.country_code.upper(): [
            d.value for d in entry.domain if d.value and d.type != 1
        ]
        for entry in geo.entry
        if entry.country_code
    }
    geo.Clear()   # освобождаем C-память protobuf объекта
    return result


# ─── Parallel GitHub Release Fetcher ─────────────────────────────────────────

def _download(url: str) -> bytes:
    """Скачивает URL; при SSLError повторяет без верификации."""
    try:
        r = session.get(url, timeout=90)
        r.raise_for_status()
        return r.content
    except requests.exceptions.SSLError as e:
        log.warning("SSL error, retrying without verify: %s", e)
        r = session.get(url, timeout=90, verify=False)
        r.raise_for_status()
        return r.content


def fetch_release_assets(api_url: str, want: set[str] | None = None) -> dict[str, bytes]:
    """
    Загружает asset'ы последнего релиза параллельно.

    Args:
        api_url: GitHub API releases/latest URL.
        want:    Имена нужных файлов. None — все файлы.

    Returns:
        {filename: content}
    """
    log.info("Fetching release index: %s", api_url)
    resp = session.get(api_url, timeout=15)
    resp.raise_for_status()

    to_fetch = [
        (asset["name"], asset["browser_download_url"])
        for asset in resp.json().get("assets", [])
        if want is None or asset["name"] in want
    ]

    for name in (want or set()) - {n for n, _ in to_fetch}:
        log.warning("  Asset not found in release: %s", name)

    def _fetch_one(item: tuple[str, str]) -> tuple[str, bytes | None]:
        name, url = item
        log.info("  ↓ %s", name)
        try:
            return name, _download(url)
        except Exception as e:
            log.warning("  Failed %s: %s", name, e)
            return name, None

    result: dict[str, bytes] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        for name, content in pool.map(_fetch_one, to_fetch):
            if content is not None:
                result[name] = content

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
        return [ln.strip() for ln in r.text.splitlines() if ln.strip() and not ln.startswith("#")]
    except Exception as e:
        log.warning("Failed to fetch Telegram IPs: %s", e)
        return []


def fetch_url_ips(url: str) -> list[str]:
    try:
        r = session.get(url, timeout=15)
        return [ln.strip() for ln in r.text.splitlines() if ln.strip() and not ln.startswith("#")]
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
    out:  list[str] = []
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


def parse_txt_domains(content: bytes) -> list[str]:
    return [
        ln for line in content.decode("utf-8", errors="ignore").splitlines()
        if (ln := line.strip()) and not ln.startswith("#")
    ]


def merge_geosite(
    base: dict[str, list[str]],
    *extras: dict[str, list[str]],
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {k: list(v) for k, v in base.items()}
    for extra in extras:
        for tag, domains in extra.items():
            merged.setdefault(tag, []).extend(domains)
    log.info("Merged geosite: %d tags total", len(merged))
    return merged


def load_yaml(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


# ─── Builders ─────────────────────────────────────────────────────────────────

def build_ru_only(
    geosite: dict[str, list[str]],
    blocked_txt: dict[str, list[str]],
    ru_only_dat: dict[str, list[str]] | None,
) -> None:
    log.info("─ Building ru-only list")
    domains: list[str] = []

    for tag in RU_ONLY_TAGS:
        chunk = geosite.get(tag, [])
        log.info("  [geosite/%s] %d domains", tag, len(chunk))
        domains.extend(chunk)

    if ru_only_dat:
        total = sum(len(v) for v in ru_only_dat.values())
        log.info("  [geosite-ru-only.dat] %d domains / %d tags", total, len(ru_only_dat))
        for d_list in ru_only_dat.values():
            domains.extend(d_list)

    for filename, slug in BLOCKED_TXT_ROUTING.items():
        if slug == "ru-only" and filename in blocked_txt:
            chunk = blocked_txt[filename]
            log.info("  [%s] %d domains", filename, len(chunk))
            domains.extend(chunk)

    extra_file = CONFIG / "ru_extra.txt"
    if extra_file.exists():
        extras = [
            ln.strip()
            for ln in extra_file.read_text().splitlines()
            if ln.strip() and not ln.startswith("#")
        ]
        log.info("  [ru_extra.txt] %d domains", len(extras))
        domains.extend(extras)

    write_output(OUTPUT / "ru-only" / "domains.txt", normalize(domains))


def build_services(
    geosite: dict[str, list[str]],
    blocked_txt: dict[str, list[str]],
    ip_sources: dict,
    extra_domains: dict,
) -> None:
    all_slugs: set[str] = {tag.lower() for tag in geosite if tag not in RU_ONLY_TAGS}
    for slug in BLOCKED_TXT_ROUTING.values():
        if slug and slug != "ru-only":
            all_slugs.add(slug)

    log.info("─ Building per-service lists (%d services)", len(all_slugs))

    ip_cache: dict[str, list[str]] = {}

    def get_ips(source_key: str, source_cfg) -> list[str]:
        if source_key not in ip_cache:
            if isinstance(source_cfg, str):
                fetcher = _ip_fetchers.get(source_cfg)
                ip_cache[source_key] = fetcher() if fetcher else []
            elif isinstance(source_cfg, dict) and source_cfg.get("url"):
                ip_cache[source_key] = fetch_url_ips(source_cfg["url"])
            else:
                ip_cache[source_key] = []
        return ip_cache[source_key]

    for slug in sorted(all_slugs):
        tag = slug.upper()
        all_domains: list[str] = list(geosite.get(tag, []))

        for filename, routed_slug in BLOCKED_TXT_ROUTING.items():
            if routed_slug == slug and filename in blocked_txt:
                chunk = blocked_txt[filename]
                log.info("  [%s] +%d domains from %s", slug, len(chunk), filename)
                all_domains.extend(chunk)

        extras = extra_domains.get(slug, []) or extra_domains.get(tag, [])
        if extras:
            log.info("  [%s] +%d extra domains", slug, len(extras))
            all_domains.extend(extras)

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
    ip_sources    = load_yaml(CONFIG / "ip_sources.yml")
    extra_domains = load_yaml(CONFIG / "extra_domains.yml")

    blocked_want = {"geosite.dat", "geosite-ru-only.dat"} | set(BLOCKED_TXT_ROUTING.keys())

    # Качаем оба репозитория параллельно
    log.info("=== Fetching sources (parallel) ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_v2ray   = pool.submit(fetch_release_assets, V2RAY_RULES_API,     {"geosite.dat"})
        f_blocked = pool.submit(fetch_release_assets, BLOCKED_GEOSITE_API, blocked_want)
        v2ray_assets   = f_v2ray.result()
        blocked_assets = f_blocked.result()

    # Парсим последовательно — каждый файл освобождаем сразу после парсинга.
    # Protobuf держит C-память отдельно от Python heap: явный del + geo.Clear()
    # обязательны, иначе пик памяти = сумма всех файлов одновременно (~400+ MB).
    log.info("=== Parsing geosite files ===")

    geosite_v2ray: dict[str, list[str]] = {}
    if "geosite.dat" in v2ray_assets:
        geosite_v2ray = parse_geosite_dat(v2ray_assets.pop("geosite.dat"))
        log.info("v2ray geosite.dat: %d tags", len(geosite_v2ray))
    del v2ray_assets   # сырые байты больше не нужны

    geosite_blocked: dict[str, list[str]] = {}
    if "geosite.dat" in blocked_assets:
        geosite_blocked = parse_geosite_dat(blocked_assets.pop("geosite.dat"))
        log.info("blocked geosite.dat: %d tags", len(geosite_blocked))

    ru_only_dat: dict[str, list[str]] | None = None
    if "geosite-ru-only.dat" in blocked_assets:
        ru_only_dat = parse_geosite_dat(blocked_assets.pop("geosite-ru-only.dat"))
        log.info("geosite-ru-only.dat: %d tags", len(ru_only_dat))

    blocked_txt: dict[str, list[str]] = {
        filename: parse_txt_domains(blocked_assets.pop(filename))
        for filename in list(BLOCKED_TXT_ROUTING)
        if filename in blocked_assets
    }
    del blocked_assets   # сырые байты больше не нужны

    # Мёрджим и строим выходные файлы
    geosite = merge_geosite(geosite_v2ray, geosite_blocked)
    del geosite_v2ray, geosite_blocked   # освобождаем промежуточные словари

    build_ru_only(geosite, blocked_txt, ru_only_dat)
    build_services(geosite, blocked_txt, ip_sources, extra_domains)

    log.info("Done! Output → %s/", OUTPUT.relative_to(BASE))


if __name__ == "__main__":
    main()
