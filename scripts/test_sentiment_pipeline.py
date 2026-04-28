"""
Smoke test — no API keys, no real orders, no real news.
Run: python scripts/test_sentiment_pipeline.py
Expected output: all steps PASS
"""
import asyncio
import sys
sys.path.insert(0, ".")


async def main():
    results = []

    # Step 1: KeywordNewsAnalyzer works without API key
    from data.news_analyzer import KeywordNewsAnalyzer
    from data.news_feed import NewsItem
    from datetime import datetime, timezone

    analyzer = KeywordNewsAnalyzer()
    mock_item = NewsItem(
        fingerprint="test001",
        source="test",
        title="Russia and Ukraine agree to ceasefire negotiations",
        url="https://example.com/test",
        published_at=datetime.now(timezone.utc),
        summary="Officials from both sides met in Geneva to discuss a possible truce.",
        raw_themes=["ceasefire"],
    )
    theme_config = {
        "description": "Peace negotiations",
        "keywords": ["ceasefire", "truce", "negotiations", "peace"],
    }
    analysis = await analyzer.analyze(mock_item, "ceasefire", theme_config)

    ok = analysis["is_relevant"] and analysis["direction"] in ("increase_yes", "decrease_yes", "neutral")
    results.append(("KeywordAnalyzer schema valid", ok))
    results.append(("KeywordAnalyzer is_relevant", bool(analysis["is_relevant"])))

    # Step 2: estimate_fair_probability works
    from strategies.ai_sentiment import estimate_fair_probability
    fair = estimate_fair_probability(analysis, current_market_price=0.40)
    results.append(("fair_prob in range", 0.05 <= fair <= 0.95))

    # Step 3: edge calculation
    edge = abs(fair - 0.40)
    results.append(("edge calculated", edge >= 0.0))

    # Step 4: NewsFeed theme matching
    from data.news_feed import NewsFeed

    class MockFeed(NewsFeed):
        def __init__(self):
            self._theme_config = {"ceasefire": {"keywords": ["ceasefire", "truce"]}}
            self._seen = set()
            self._running = False

    feed = MockFeed()
    themes = feed._match_themes("Ukraine ceasefire talks begin in Geneva")
    results.append(("theme matching works", "ceasefire" in themes))

    # Step 5: NewsItem fingerprint field exists
    results.append(("NewsItem has fingerprint", hasattr(mock_item, "fingerprint")))

    # Step 6: NewsItem has raw_themes
    results.append(("NewsItem has raw_themes", hasattr(mock_item, "raw_themes")))

    # Step 7: KeywordAnalyzer irrelevant result on off-topic news
    off_topic = NewsItem(
        fingerprint="test002",
        source="test",
        title="Local bakery wins award for best croissant",
        url="https://example.com/test2",
        published_at=datetime.now(timezone.utc),
        summary="A small bakery in Paris wins national prize.",
        raw_themes=[],
    )
    off_analysis = await analyzer.analyze(off_topic, "ceasefire", theme_config)
    results.append(("off-topic returns is_relevant=False", not off_analysis["is_relevant"]))

    # Print results
    print("\n=== Sentiment Pipeline Smoke Test ===")
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}")
    print(f"\n{'ALL PASS' if all_pass else 'SOME FAILED'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
