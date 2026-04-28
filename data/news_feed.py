"""News feed: YAML-configured RSS + NewsAPI + Twitter/X polling with theme-aware filtering."""
from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import aiohttp
import feedparser

from config import settings
from utils.logger import logger

# Accounts to monitor on Twitter/X — major breaking news sources
TWITTER_ACCOUNTS = [
    "Reuters",
    "AP",
    "BBCBreaking",
    "Reuters_Biz",
    "federalreserve",
    "WhiteHouse",
    "realDonaldTrump",
    "ElonMusk",
    "coindesk",
    "CoinTelegraph",
    "POTUS",
]

# How often to poll Twitter (seconds) — free tier: 1 req/15min per endpoint,
# but user-timeline lookup is per-account so we space calls to avoid rate limits.
TWITTER_POLL_INTERVAL = 60   # check each account every 60s in the background loop


@dataclass
class NewsItem:
    fingerprint: str          # SHA-256[:16] of title+url for deduplication
    title: str
    summary: str
    source: str
    url: str = ""
    published_at: Optional[datetime] = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_themes: list = field(default_factory=list)   # theme names matched by keyword pre-filter


DEFAULT_RSS_FEEDS = [
    # ── Existing feeds ─────────────────────────────────────────────────────────
    {"name": "bbc_world", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "reuters_world", "url": "https://feeds.reuters.com/reuters/worldnews"},
    {"name": "reuters_business", "url": "https://feeds.reuters.com/reuters/businessNews", "category": "economics"},
    {"name": "reuters_top", "url": "https://feeds.reuters.com/reuters/topNews", "category": "economics"},
    {"name": "ap_sports", "url": "https://apnews.com/apf-sports", "category": "sports"},
    {"name": "espn", "url": "http://www.espn.com/espn/rss/news", "category": "sports"},

    # ── Fast-breaking general news ────────────────────────────────────────────
    {"name": "guardian_world", "url": "https://www.theguardian.com/world/rss"},
    {"name": "guardian_us", "url": "https://www.theguardian.com/us-news/rss", "category": "politics"},
    {"name": "aljazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml"},

    # ── Finance / markets ──────────────────────────────────────────────────────
    {"name": "bloomberg_markets", "url": "https://feeds.bloomberg.com/markets/news.rss", "category": "economics"},
    {"name": "cnbc_top", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "category": "economics"},
    {"name": "ft_markets", "url": "https://www.ft.com/markets?format=rss", "category": "economics"},

    # ── Crypto ────────────────────────────────────────────────────────────────
    {"name": "coindesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "category": "crypto"},
    {"name": "cointelegraph", "url": "https://cointelegraph.com/rss", "category": "crypto"},
    {"name": "decrypt", "url": "https://decrypt.co/feed", "category": "crypto"},

    # ── US Government / Policy ─────────────────────────────────────────────────
    {"name": "fed_pressreleases", "url": "https://www.federalreserve.gov/feeds/press_all.xml", "category": "economics"},
]


class NewsFeed:
    """
    Polls RSS feeds, NewsAPI, and Twitter/X every ~60 seconds.
    Deduplicates via fingerprints.
    Calls `on_item` for each new, non-stale, theme-matched headline.
    """

    def __init__(
        self,
        on_item: Callable[[NewsItem], None],
        theme_config: dict = None,
        news_config: dict | None = None,
    ):
        self._on_item = on_item
        self._theme_config = theme_config or {}
        self._news_config = news_config or {}
        self._seen: set[str] = set()
        self._running = False
        self._twitter_client = None
        self._twitter_last_ids: dict[str, int] = {}  # username → newest tweet id seen
        self._twitter_initialized = False

    def _init_twitter(self) -> bool:
        """
        Lazily initialise tweepy client. Returns True if ready.

        NOTE: Twitter API v2 read access requires the Basic plan ($100/month).
        The free developer tier is write-only since the 2023 pricing change.
        If the token is set but the tier is insufficient, we log once and skip.
        Real-time breaking news is covered instead by the expanded RSS feed list
        (AP, Guardian, Bloomberg, CoinDesk, Federal Reserve, Truth Social, etc.).
        """
        if self._twitter_initialized:
            return self._twitter_client is not None
        self._twitter_initialized = True
        token = settings.TWITTER_BEARER_TOKEN
        if not token:
            return False
        try:
            import tweepy
            client = tweepy.Client(bearer_token=token, wait_on_rate_limit=False)
            # Probe the API with a minimal call to detect 402 before the main loop
            test = client.get_user(username="Reuters")
            self._twitter_client = client
            logger.info(
                f"TwitterFeed: active — monitoring {len(TWITTER_ACCOUNTS)} accounts: "
                + ", ".join(f"@{a}" for a in TWITTER_ACCOUNTS)
            )
            return True
        except ImportError:
            logger.warning("TwitterFeed: tweepy not installed — run: pip install tweepy")
            return False
        except Exception as exc:
            err = str(exc)
            if "402" in err or "Payment Required" in err:
                logger.info(
                    "TwitterFeed: disabled — Twitter API read access requires the Basic plan "
                    "($100/month). Using expanded RSS feeds instead "
                    "(AP, Bloomberg, CoinDesk, FedReserve, TruthSocial, etc.)."
                )
            else:
                logger.warning(f"TwitterFeed: init failed: {exc}")
            return False

    async def run(self) -> None:
        self._running = True
        while self._running:
            await asyncio.gather(
                self._poll_rss(),
                self._poll_newsapi(),
                self._poll_twitter(),
                return_exceptions=True,
            )
            await asyncio.sleep(settings.SENTIMENT_POLL_INTERVAL)

    def _rss_feeds(self) -> list[tuple[str, str]]:
        configured = self._news_config.get("rss_feeds") or DEFAULT_RSS_FEEDS
        feeds: list[tuple[str, str]] = []
        for item in configured:
            if isinstance(item, dict):
                if item.get("enabled") is False:
                    continue
                category = str(item.get("category") or "").strip().lower()
                if category and category in self._blocked_categories():
                    continue
                name = str(item.get("name") or item.get("url") or "rss")
                url = str(item.get("url") or "").strip()
            else:
                name = "rss"
                url = str(item or "").strip()
            if url:
                feeds.append((name, url))
        return feeds

    def _rss_enabled(self) -> bool:
        return bool(self._news_config.get("rss_enabled", True))

    def _newsapi_enabled(self) -> bool:
        return bool(self._news_config.get("newsapi_enabled", True))

    def _blocked_categories(self) -> set[str]:
        values = self._news_config.get("blocked_categories") or []
        return {str(value).strip().lower() for value in values if str(value).strip()}

    def _blocked_source_keywords(self) -> list[str]:
        values = self._news_config.get("blocked_source_keywords") or []
        return [str(value).strip().lower() for value in values if str(value).strip()]

    def _source_allowed(self, source_name: str) -> bool:
        source_text = (source_name or "").lower()
        for keyword in self._blocked_source_keywords():
            if keyword in source_text:
                return False
        return True

    async def _poll_rss(self) -> None:
        if not self._rss_enabled():
            return
        loop = asyncio.get_event_loop()
        for feed_name, url in self._rss_feeds():
            try:
                feed = await loop.run_in_executor(None, feedparser.parse, url)
                for entry in feed.entries[:10]:
                    title = entry.get("title", "")
                    entry_url = entry.get("link", entry.get("id", ""))
                    fp = _fingerprint(title, entry_url)
                    if fp in self._seen:
                        continue
                    self._seen.add(fp)

                    summary = entry.get("summary", entry.get("description", ""))[:500]
                    published_at = _parse_feedparser_date(entry.get("published_parsed"))
                    themes = self._match_themes(title + " " + summary)

                    # Skip items with no matching theme when theme config is loaded
                    if self._theme_config and not themes:
                        continue

                    source_name = feed.feed.get("title", feed_name)
                    if not self._source_allowed(source_name):
                        continue

                    item = NewsItem(
                        fingerprint=fp,
                        title=title,
                        summary=summary,
                        source=source_name,
                        url=entry_url,
                        published_at=published_at,
                        raw_themes=themes,
                    )
                    self._on_item(item)
            except Exception as exc:
                logger.warning(f"RSS poll failed for {url}: {exc}")

    async def _poll_newsapi(self) -> None:
        if not self._newsapi_enabled():
            return
        if not settings.NEWS_API_KEY or settings.NEWS_API_KEY == "your-newsapi-key":
            return
        api_url = "https://newsapi.org/v2/top-headlines"
        params = {
            "apiKey": settings.NEWS_API_KEY,
            "language": "en",
            "pageSize": 20,
            "category": "general",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    for article in data.get("articles", []):
                        title = article.get("title", "")
                        article_url = article.get("url", "")
                        fp = _fingerprint(title, article_url)
                        if fp in self._seen:
                            continue
                        self._seen.add(fp)

                        summary = (article.get("description") or "")[:500]
                        pub_str = article.get("publishedAt", "")
                        try:
                            published_at = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                        except Exception:
                            published_at = datetime.now(timezone.utc)
                        themes = self._match_themes(title + " " + summary)

                        if self._theme_config and not themes:
                            continue

                        source_name = (article.get("source") or {}).get("name", "NewsAPI")
                        if not self._source_allowed(source_name):
                            continue

                        item = NewsItem(
                            fingerprint=fp,
                            title=title,
                            summary=summary,
                            source=source_name,
                            url=article_url,
                            published_at=published_at,
                            raw_themes=themes,
                        )
                        self._on_item(item)
        except Exception as exc:
            logger.warning(f"NewsAPI poll failed: {exc}")

    async def _poll_twitter(self) -> None:
        """
        Polls recent tweets from TWITTER_ACCOUNTS using the Twitter API v2 free tier.
        Uses since_id to only fetch new tweets since the last poll, so we never process
        the same tweet twice. Runs in a thread executor to avoid blocking the event loop.
        """
        if not self._init_twitter():
            return

        loop = asyncio.get_event_loop()
        accounts = self._news_config.get("twitter_accounts") or TWITTER_ACCOUNTS

        for username in accounts:
            try:
                await loop.run_in_executor(
                    None, self._fetch_user_tweets, username
                )
                # Small gap between accounts to stay well within rate limits
                await asyncio.sleep(1)
            except Exception as exc:
                logger.debug(f"TwitterFeed: poll error for @{username}: {exc}")

    def _fetch_user_tweets(self, username: str) -> None:
        """
        Synchronous tweet fetch for one account (called from executor).
        Fetches up to 10 most recent tweets, only those newer than last seen id.
        """
        try:
            import tweepy

            # Resolve username → user id (cached implicitly by tweepy)
            user_resp = self._twitter_client.get_user(username=username)
            if not user_resp or not user_resp.data:
                return
            user_id = user_resp.data.id

            kwargs: dict = {
                "max_results": 10,
                "tweet_fields": ["created_at", "text", "entities"],
                "expansions": ["attachments.media_keys"],
            }
            since_id = self._twitter_last_ids.get(username)
            if since_id:
                kwargs["since_id"] = since_id

            resp = self._twitter_client.get_users_tweets(user_id, **kwargs)
            if not resp or not resp.data:
                return

            # Tweets come newest-first; process oldest-first so since_id tracks correctly
            tweets = list(reversed(resp.data))
            new_max_id = self._twitter_last_ids.get(username, 0)

            for tweet in tweets:
                tweet_id = tweet.id
                text = tweet.text or ""

                # Skip retweets — they're already covered by the original account
                if text.startswith("RT @"):
                    new_max_id = max(new_max_id, tweet_id)
                    continue

                # Build a tweet URL for deduplication and linking
                tweet_url = f"https://twitter.com/{username}/status/{tweet_id}"
                fp = _fingerprint(text[:120], tweet_url)
                if fp in self._seen:
                    new_max_id = max(new_max_id, tweet_id)
                    continue
                self._seen.add(fp)

                # Extract any linked URL from entities (article the tweet links to)
                linked_url = ""
                entities = tweet.entities or {}
                urls = entities.get("urls", []) if isinstance(entities, dict) else getattr(entities, "urls", []) or []
                for u in urls:
                    expanded = (u.get("expanded_url") if isinstance(u, dict) else getattr(u, "expanded_url", "")) or ""
                    # Skip t.co wrappers and twitter.com self-links
                    if expanded and "twitter.com" not in expanded and "t.co" not in expanded:
                        linked_url = expanded
                        break

                published_at = datetime.now(timezone.utc)
                if hasattr(tweet, "created_at") and tweet.created_at:
                    published_at = tweet.created_at
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)

                themes = self._match_themes(text)
                # On Twitter, theme filter is relaxed slightly — short tweets may not
                # hit keywords but are still worth passing for LLM triage if from a
                # high-signal account (Reuters, AP, BBCBreaking, federalreserve, POTUS)
                high_signal = username.lower() in {
                    "reuters", "ap", "bbcbreaking", "reuters_biz",
                    "federalreserve", "whitehouse", "potus",
                }
                if self._theme_config and not themes and not high_signal:
                    new_max_id = max(new_max_id, tweet_id)
                    continue

                source_label = f"Twitter/@{username}"
                item = NewsItem(
                    fingerprint=fp,
                    title=text[:280],          # tweet text as headline
                    summary=text[:500],        # same text; Groq gets full context
                    source=source_label,
                    url=linked_url or tweet_url,
                    published_at=published_at,
                    raw_themes=themes,
                )
                logger.debug(f"TwitterFeed: new tweet from @{username}: {text[:80]}…")
                self._on_item(item)
                new_max_id = max(new_max_id, tweet_id)

            if new_max_id:
                self._twitter_last_ids[username] = new_max_id

        except Exception as exc:
            # 429 = rate limited; 403 = account protected; log at debug to avoid spam
            err_str = str(exc)
            if "429" in err_str or "Rate limit" in err_str.lower():
                logger.debug(f"TwitterFeed: rate limited for @{username} — will retry next cycle")
            elif "403" in err_str:
                logger.debug(f"TwitterFeed: @{username} is protected/forbidden — skipping")
            else:
                logger.debug(f"TwitterFeed: fetch failed for @{username}: {exc}")

    def _match_themes(self, text: str) -> list:
        """Return list of theme names whose keywords appear in text."""
        if not self._theme_config:
            return []
        text_lower = text.lower()
        matched = []
        for theme_name, theme_data in self._theme_config.items():
            if not theme_data.get("enabled", True):
                continue
            keywords = theme_data.get("keywords", [])
            if any(kw.lower() in text_lower for kw in keywords):
                matched.append(theme_name)
        return matched

    async def stop(self) -> None:
        self._running = False


def _fingerprint(title: str, url: str) -> str:
    return hashlib.sha256(f"{title}{url}".encode()).hexdigest()[:16]


def _parse_feedparser_date(parsed_date) -> datetime:
    """Convert feedparser time.struct_time to datetime, fallback to now."""
    if parsed_date is None:
        return datetime.now(timezone.utc)
    try:
        from time import mktime
        return datetime.fromtimestamp(mktime(parsed_date), tz=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)
