"""News analysis — LLM (Claude Haiku) and keyword fallback analyzers."""
from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod

import anthropic

from config import settings
from utils.logger import logger


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class NewsAnalyzerBase(ABC):
    @abstractmethod
    async def analyze(self, news_item, theme: str, theme_config: dict) -> dict:
        """
        Returns dict with keys:
          is_relevant: bool
          theme: str
          direction: "increase_yes" | "decrease_yes" | "neutral"
          catalyst_class: "official_catalyst" | "confirmed_event" | "policy_action" |
                          "commentary" | "analysis" | "speculation" | "noise"
          confidence: float 0-1
          urgency: float 0-1
          impact_strength: float 0-1
          reasoning_short: str (max 100 chars)
          market_tags: list[str]
          analyzer_name: str
          implied_probability_shift: float  # estimated shift in probability
        """

    async def generate_market_queries(self, news_item) -> list[str]:
        """Generate Polymarket search query strings from a news item. Override in LLM subclass."""
        return []

    async def evaluate_market_pricing(
        self,
        question: str,
        current_price: float,
        hours_remaining: float,
        category: str,
        crypto_prices: dict | None = None,
    ) -> dict:
        """Evaluate if a prediction market is mispriced. Override in LLM subclass."""
        return _irrelevant_result(category, "base")


class LLMNewsAnalyzer(NewsAnalyzerBase):
    """
    Uses Claude Haiku for cheap, fast news classification.
    Falls back gracefully on rate limit or parse error.
    """

    def __init__(self):
        self._client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
        self._call_count = 0
        self._last_call_time: float = 0.0

    async def analyze(self, news_item, theme: str, theme_config: dict) -> dict:
        # Rate limiting — minimum 1 second between calls
        elapsed = time.time() - self._last_call_time
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)

        title = getattr(news_item, "title", "") or (news_item.get("title", "") if isinstance(news_item, dict) else "")
        summary = getattr(news_item, "summary", "") or (news_item.get("summary", "") if isinstance(news_item, dict) else "")
        description = theme_config.get("description", theme)

        prompt = f"""You are a prediction market classifier. Analyze this news for theme: {theme}.
Theme description: {description}

Headline: {title}
Summary: {summary[:300]}

Respond ONLY with valid JSON (no markdown, no explanation):
{{
  "is_relevant": true or false,
  "direction": "increase_yes" or "decrease_yes" or "neutral",
  "catalyst_class": "official_catalyst" or "confirmed_event" or "policy_action" or "commentary" or "analysis" or "speculation" or "noise",
  "confidence": 0.0 to 1.0,
  "urgency": 0.0 to 1.0,
  "impact_strength": 0.0 to 1.0,
  "reasoning_short": "max 80 chars",
  "market_tags": ["tag1", "tag2"],
  "implied_probability_shift": -0.25 to 0.25
}}

Rules:
- is_relevant: true only if this directly affects {theme} outcomes
- direction: increase_yes = YES bets become more likely; decrease_yes = less likely
- catalyst_class:
  * official_catalyst = direct official action, ruling, signed agreement, confirmed statement from a key actor
  * confirmed_event = verified event that directly changes the market odds
  * policy_action = sanctions, tariffs, court action, parliamentary/executive/regulatory step
  * commentary / analysis / speculation / noise = opinion, recap, background, punditry, vague forward-looking talk
- confidence: your certainty in the directional call
- urgency: 1.0 = breaking/immediate, 0.1 = background context
- impact_strength: 1.0 = decisive event, 0.1 = marginal signal
- implied_probability_shift: estimated raw shift in probability before any later calibration (positive = toward YES)
- Do NOT anchor on 0.18 or any other default number. Choose the smallest justified magnitude.
- Use this magnitude guide:
  * 0.00 to 0.03 = minor/background/contextual update or follow-up reporting
  * 0.03 to 0.08 = meaningful but non-decisive update
  * 0.08 to 0.15 = strong breaking development with direct market relevance
  * 0.15 to 0.25 = rare decisive official event that nearly resolves the outcome
- High confidence alone does NOT justify a large shift. Larger shifts require both high urgency and high impact_strength.
- Repeated commentary, summaries, and incremental developments should usually be 0.05 or smaller.
- Commentary, analysis, speculation, and vague geopolitical framing should almost never be treated as tradable catalysts.
- Make the sign match direction: increase_yes => positive shift, decrease_yes => negative shift, neutral => 0.0
- If not relevant: all scores 0.0, direction neutral"""

        try:
            response = await self._client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            self._call_count += 1
            self._last_call_time = time.time()
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            data = json.loads(text)
            is_relevant = bool(data.get("is_relevant", False))
            direction = data.get("direction", "neutral")
            catalyst_class = str(data.get("catalyst_class", "noise")).strip().lower()
            if catalyst_class not in {
                "official_catalyst",
                "confirmed_event",
                "policy_action",
                "commentary",
                "analysis",
                "speculation",
                "noise",
            }:
                catalyst_class = "noise"
            confidence = _clamp(float(data.get("confidence", 0.0)), 0.0, 1.0)
            urgency = _clamp(float(data.get("urgency", 0.0)), 0.0, 1.0)
            impact_strength = _clamp(float(data.get("impact_strength", 0.0)), 0.0, 1.0)
            implied_shift = _clamp(float(data.get("implied_probability_shift", 0.0)), -0.25, 0.25)
            if not is_relevant or direction == "neutral":
                implied_shift = 0.0
            elif direction == "increase_yes":
                implied_shift = abs(implied_shift)
            elif direction == "decrease_yes":
                implied_shift = -abs(implied_shift)
            return {
                "is_relevant": is_relevant,
                "theme": theme,
                "direction": direction,
                "catalyst_class": catalyst_class,
                "confidence": confidence,
                "urgency": urgency,
                "impact_strength": impact_strength,
                "reasoning_short": str(data.get("reasoning_short", ""))[:100],
                "market_tags": data.get("market_tags", []),
                "analyzer_name": "llm_haiku",
                "implied_probability_shift": implied_shift,
            }
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"LLMNewsAnalyzer parse error: {e}")
            return _irrelevant_result(theme, "llm_haiku")
        except anthropic.RateLimitError:
            logger.warning("LLMNewsAnalyzer: rate limited — sleeping 5s")
            await asyncio.sleep(5)
            return _irrelevant_result(theme, "llm_haiku")
        except Exception as e:
            logger.error(f"LLMNewsAnalyzer error: {e}")
            return _irrelevant_result(theme, "llm_haiku")

    async def generate_market_queries(self, news_item) -> list[str]:
        """
        Ask Claude Haiku to generate Polymarket-style search queries for this headline.
        Returns up to 5 short query strings (2-6 words each) that match market titles.
        """
        elapsed = time.time() - self._last_call_time
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)

        title = getattr(news_item, "title", "") or ""
        summary = getattr(news_item, "summary", "") or ""

        prompt = f"""You are finding prediction markets on Polymarket for a news headline.

Headline: {title}
Summary: {summary[:250]}

Generate 4 short search query strings (2-6 words each) that would match active Polymarket
prediction market titles for this news. Focus on:
- The specific outcome being predicted (price target, event happening, decision made)
- Concrete actors and actions (e.g. "fed rate cut", "trump tariff china", "bitcoin 100k")
- Date ranges if relevant (e.g. "may 2026", "Q2 2026")

Respond ONLY with a JSON array of strings, no markdown, no explanation.
Example: ["fed rate cut may 2026", "US interest rate 2026", "jerome powell rate decision", "fomc meeting may"]"""

        try:
            response = await self._client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=120,
                messages=[{"role": "user", "content": prompt}],
            )
            self._call_count += 1
            self._last_call_time = time.time()
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            queries = json.loads(text)
            if isinstance(queries, list):
                return [str(q).strip() for q in queries if str(q).strip()][:5]
            return []
        except Exception as e:
            logger.debug(f"LLMNewsAnalyzer: generate_market_queries failed: {e}")
            return []

    async def evaluate_market_pricing(
        self,
        question: str,
        current_price: float,
        hours_remaining: float,
        category: str,
        crypto_prices: dict | None = None,
    ) -> dict:
        """
        Ask Claude Haiku whether a prediction market is fairly priced.
        Returns the same dict format as analyze(), but based on market assessment
        rather than a news event. Used by the proactive scanner.
        """
        elapsed = time.time() - self._last_call_time
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)

        prompt = f"""You are evaluating a prediction market for mispricing.

Market question: {question}
Current market price: {current_price:.3f} (implies {current_price*100:.1f}% probability of YES)
Time until resolution: {hours_remaining:.1f} hours
Category: {category}

Based on your knowledge, estimate the TRUE probability this market resolves YES.
Then determine if the market is mispriced.

Respond ONLY with valid JSON (no markdown):
{{
  "is_mispriced": true or false,
  "direction": "buy_yes" or "buy_no" or "pass",
  "fair_probability": 0.0 to 1.0,
  "confidence": 0.0 to 1.0,
  "reasoning": "max 80 chars"
}}

Rules:
- fair_probability: your honest estimate of YES probability given current world state
- is_mispriced: true only if |fair_probability - {current_price:.3f}| >= 0.05
- direction: buy_yes if fair_prob > current_price + 0.05, buy_no if fair_prob < current_price - 0.05, else pass
- confidence: 0 = no knowledge, 1 = very certain (be conservative — most markets are close to fair)
- If the market is about very recent events you have no data on, set confidence=0.1 and direction=pass
- Never invent or assume outcomes — only trade when you have clear directional knowledge"""

        try:
            response = await self._client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            self._call_count += 1
            self._last_call_time = time.time()
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            data = json.loads(text)
            is_mispriced = bool(data.get("is_mispriced", False))
            direction = str(data.get("direction", "pass")).strip().lower()
            fair_prob = _clamp(float(data.get("fair_probability", current_price)), 0.0, 1.0)
            confidence = _clamp(float(data.get("confidence", 0.0)), 0.0, 1.0)
            reasoning = str(data.get("reasoning", ""))[:100]

            if not is_mispriced or direction == "pass":
                implied_shift = 0.0
                sent_direction = "neutral"
            elif direction == "buy_yes":
                implied_shift = fair_prob - current_price
                sent_direction = "increase_yes"
            else:  # buy_no
                implied_shift = fair_prob - current_price  # negative
                sent_direction = "decrease_yes"

            return {
                "is_relevant": is_mispriced and direction != "pass",
                "theme": category,
                "direction": sent_direction,
                "catalyst_class": "confirmed_event",
                "confidence": confidence,
                "urgency": min(1.0, 6.0 / max(hours_remaining, 0.5)),
                "impact_strength": confidence,
                "reasoning_short": reasoning,
                "market_tags": [category],
                "analyzer_name": "llm_haiku_pricing",
                "implied_probability_shift": implied_shift,
                "fair_probability": fair_prob,
            }
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"LLMNewsAnalyzer.evaluate_market_pricing parse error: {e}")
            return _irrelevant_result(category, "llm_haiku_pricing")
        except anthropic.RateLimitError:
            await asyncio.sleep(5)
            return _irrelevant_result(category, "llm_haiku_pricing")
        except Exception as e:
            logger.debug(f"LLMNewsAnalyzer.evaluate_market_pricing error: {e}")
            return _irrelevant_result(category, "llm_haiku_pricing")


class KeywordNewsAnalyzer(NewsAnalyzerBase):
    """
    Zero-API fallback. Uses keyword counting and direction word lists.
    Activated when ANTHROPIC_API_KEY is not set.
    """

    URGENCY_WORDS = {"breaking", "urgent", "flash", "crisis", "alert", "sudden",
                     "collapse", "shock", "emergency", "immediate"}
    ESCALATION_WORDS = {"attack", "strike", "escalation", "conflict", "war",
                        "explosion", "missile", "sanction", "ban", "rejected"}
    DE_ESCALATION_WORDS = {"ceasefire", "deal", "agreement", "resolution",
                           "truce", "signed", "approved", "settled", "peace"}
    OFFICIAL_WORDS = {"official", "ministry", "court", "supreme court", "government",
                      "white house", "president", "prime minister", "signed", "approved",
                      "announced", "confirmed", "ordered"}
    POLICY_WORDS = {"sanction", "tariff", "regulation", "bill", "court", "executive order",
                    "parliament", "congress", "fed", "rate", "policy"}
    COMMENTARY_WORDS = {"opinion", "analysis", "commentary", "explainer", "how", "why",
                        "what it means", "outlook", "could", "may", "might"}

    async def analyze(self, news_item, theme: str, theme_config: dict) -> dict:
        title = getattr(news_item, "title", "") or (news_item.get("title", "") if isinstance(news_item, dict) else "")
        summary = getattr(news_item, "summary", "") or (news_item.get("summary", "") if isinstance(news_item, dict) else "")
        text = (title + " " + summary).lower()
        keywords = theme_config.get("keywords", [])

        hits = sum(1 for kw in keywords if kw.lower() in text)
        confidence = min(hits / max(len(keywords) * 0.4, 1), 1.0)
        urgency = 0.65 if any(w in text for w in self.URGENCY_WORDS) else 0.35
        is_relevant = confidence >= 0.25

        # Direction heuristic
        esc = sum(1 for w in self.ESCALATION_WORDS if w in text)
        deesc = sum(1 for w in self.DE_ESCALATION_WORDS if w in text)
        if esc > deesc:
            direction = "increase_yes"
            shift = confidence * 0.10
        elif deesc > esc:
            direction = "decrease_yes"
            shift = -confidence * 0.10
        else:
            direction = "neutral"
            shift = 0.0

        if any(w in text for w in self.COMMENTARY_WORDS):
            catalyst_class = "analysis"
        elif any(w in text for w in self.POLICY_WORDS):
            catalyst_class = "policy_action"
        elif any(w in text for w in self.OFFICIAL_WORDS):
            catalyst_class = "official_catalyst"
        elif esc or deesc:
            catalyst_class = "confirmed_event"
        else:
            catalyst_class = "noise"

        return {
            "is_relevant": is_relevant,
            "theme": theme,
            "direction": direction,
            "catalyst_class": catalyst_class,
            "confidence": confidence,
            "urgency": urgency,
            "impact_strength": 0.4,
            "reasoning_short": f"keyword: {hits}/{len(keywords)} hits",
            "market_tags": [kw for kw in keywords[:3]],
            "analyzer_name": "keyword",
            "implied_probability_shift": shift,
        }


class GroqNewsAnalyzer(LLMNewsAnalyzer):
    """
    Uses Groq's free API (Llama 3.3 70B) for news classification.
    Same prompts as Claude Haiku — drop-in replacement / secondary analyzer.
    Free tier: 14,400 requests/day, 6,000 tokens/min — more than enough.
    Sign up: https://console.groq.com  (free, no credit card)
    """

    def __init__(self):
        # Don't call LLMNewsAnalyzer.__init__ — it sets up Anthropic client
        NewsAnalyzerBase.__init__(self)
        self._call_count = 0
        self._last_call_time: float = 0.0
        self._groq_client = None
        try:
            import groq as groq_lib
            self._groq_client = groq_lib.AsyncGroq(api_key=settings.GROQ_API_KEY)
        except ImportError:
            logger.warning("GroqNewsAnalyzer: groq package not installed — run: pip install groq")
        except Exception as e:
            logger.warning(f"GroqNewsAnalyzer: init failed: {e}")

    # Model priority list: primary (70B, best quality) → fallback (8B, 5× daily token budget)
    # llama-3.3-70b-versatile: 100K TPD free tier
    # llama-3.1-8b-instant:    500K TPD free tier — auto-used when 70B is rate-limited
    _MODEL_PRIMARY = "llama-3.3-70b-versatile"
    _MODEL_FALLBACK = "llama-3.1-8b-instant"

    async def _call_groq(self, prompt: str, max_tokens: int = 200) -> str:
        if self._groq_client is None:
            raise RuntimeError("Groq client not initialized")
        elapsed = time.time() - self._last_call_time
        if elapsed < 0.5:
            await asyncio.sleep(0.5 - elapsed)

        model = getattr(self, "_active_model", self._MODEL_PRIMARY)
        try:
            response = await self._groq_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.1,
            )
            self._call_count += 1
            self._last_call_time = time.time()
            # If we were on fallback and primary seems to have recovered, stay on fallback
            # until next startup (avoid flip-flopping — token limits reset at midnight UTC)
            return response.choices[0].message.content.strip()
        except Exception as exc:
            err_str = str(exc)
            # 429 on primary model → switch to fallback for remainder of the day
            if "429" in err_str and model == self._MODEL_PRIMARY:
                logger.warning(
                    f"GroqNewsAnalyzer: {self._MODEL_PRIMARY} daily token limit reached — "
                    f"switching to fallback model {self._MODEL_FALLBACK} (500K TPD)"
                )
                self._active_model = self._MODEL_FALLBACK
                response = await self._groq_client.chat.completions.create(
                    model=self._MODEL_FALLBACK,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=0.1,
                )
                self._call_count += 1
                self._last_call_time = time.time()
                return response.choices[0].message.content.strip()
            raise

    async def analyze(self, news_item, theme: str, theme_config: dict) -> dict:
        if self._groq_client is None:
            return _irrelevant_result(theme, "groq_llama")
        title = getattr(news_item, "title", "") or ""
        summary = getattr(news_item, "summary", "") or ""
        description = theme_config.get("description", theme)
        # Reuse the same prompt as Claude Haiku
        from data.news_analyzer import LLMNewsAnalyzer as _Base
        prompt = (
            f"You are a prediction market classifier. Analyze this news for theme: {theme}.\n"
            f"Theme description: {description}\n\nHeadline: {title}\nSummary: {summary[:300]}\n\n"
            "Respond ONLY with valid JSON (no markdown, no explanation):\n"
            '{"is_relevant": true or false, "direction": "increase_yes" or "decrease_yes" or "neutral", '
            '"catalyst_class": "official_catalyst" or "confirmed_event" or "policy_action" or "commentary" or "analysis" or "speculation" or "noise", '
            '"confidence": 0.0 to 1.0, "urgency": 0.0 to 1.0, "impact_strength": 0.0 to 1.0, '
            '"reasoning_short": "max 80 chars", "market_tags": ["tag1", "tag2"], '
            '"implied_probability_shift": -0.25 to 0.25}'
        )
        try:
            text = await self._call_groq(prompt, max_tokens=200)
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            data = json.loads(text)
            is_relevant = bool(data.get("is_relevant", False))
            direction = data.get("direction", "neutral")
            implied_shift = _clamp(float(data.get("implied_probability_shift", 0.0)), -0.25, 0.25)
            if not is_relevant or direction == "neutral":
                implied_shift = 0.0
            elif direction == "increase_yes":
                implied_shift = abs(implied_shift)
            else:
                implied_shift = -abs(implied_shift)
            return {
                "is_relevant": is_relevant,
                "theme": theme,
                "direction": direction,
                "catalyst_class": str(data.get("catalyst_class", "noise")).strip().lower(),
                "confidence": _clamp(float(data.get("confidence", 0.0)), 0.0, 1.0),
                "urgency": _clamp(float(data.get("urgency", 0.0)), 0.0, 1.0),
                "impact_strength": _clamp(float(data.get("impact_strength", 0.0)), 0.0, 1.0),
                "reasoning_short": str(data.get("reasoning_short", ""))[:100],
                "market_tags": data.get("market_tags", []),
                "analyzer_name": "groq_llama",
                "implied_probability_shift": implied_shift,
            }
        except Exception as e:
            logger.debug(f"GroqNewsAnalyzer.analyze error: {e}")
            return _irrelevant_result(theme, "groq_llama")

    async def evaluate_market_pricing(
        self,
        question: str,
        current_price: float,
        hours_remaining: float,
        category: str,
        crypto_prices: dict | None = None,
    ) -> dict:
        if self._groq_client is None:
            return _irrelevant_result(category, "groq_llama")

        # Build live price context for crypto markets so Groq can actually
        # evaluate "Will BTC be above $X?" questions without guessing.
        price_context = ""
        if crypto_prices and category in ("crypto_markets", "crypto"):
            from data.crypto_prices import format_prices_for_prompt
            formatted = format_prices_for_prompt(crypto_prices)
            if formatted:
                price_context = (
                    f"\nLIVE CRYPTO PRICES RIGHT NOW: {formatted}\n"
                    "Use these prices to determine if the market question is already decided "
                    "or if the current probability is mispriced given where prices are.\n"
                    "For example: if BTC=$74,636 and the question asks 'Will BTC be above $70,000?', "
                    "the answer is YES with high certainty — the market should be near 1.0.\n"
                    "If the market price is 0.999 or 0.001 it is already resolved — mark is_relevant=false.\n"
                )

        prompt = (
            f"You are evaluating a prediction market for mispricing.\n"
            f"Market question: {question}\n"
            f"Current market price: {current_price:.3f} ({current_price*100:.1f}% probability YES)\n"
            f"Time until resolution: {hours_remaining:.1f} hours\nCategory: {category}\n"
            f"{price_context}\n"
            "Is this market MISPRICED? Only say yes if you have strong knowledge to assess it.\n"
            "If the market is already near-resolved (price < 0.02 or > 0.98), say is_relevant=false.\n"
            "Respond ONLY with valid JSON:\n"
            '{"is_relevant": true if mispriced else false, "direction": "increase_yes" or "decrease_yes" or "neutral", '
            '"implied_probability_shift": -0.25 to 0.25, "fair_probability": 0.0 to 1.0, '
            '"confidence": 0.0 to 1.0, "reasoning_short": "max 80 chars"}'
        )
        try:
            text = await self._call_groq(prompt, max_tokens=150)
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            data = json.loads(text)
            return {
                "is_relevant": bool(data.get("is_relevant", False)),
                "direction": data.get("direction", "neutral"),
                "implied_probability_shift": _clamp(float(data.get("implied_probability_shift", 0.0)), -0.25, 0.25),
                "fair_probability": _clamp(float(data.get("fair_probability", current_price)), 0.0, 1.0),
                "confidence": _clamp(float(data.get("confidence", 0.0)), 0.0, 1.0),
                "reasoning_short": str(data.get("reasoning_short", ""))[:100],
                "analyzer_name": "groq_llama",
            }
        except Exception as e:
            logger.warning(f"GroqNewsAnalyzer.evaluate_market_pricing error: {e}")
            return _irrelevant_result(category, "groq_llama")

    async def generate_market_queries(self, news_item) -> list[str]:
        if self._groq_client is None:
            return []
        title = getattr(news_item, "title", "") or ""
        summary = getattr(news_item, "summary", "") or ""
        prompt = (
            f"Generate 4 short Polymarket search queries (2-6 words each) for this headline.\n"
            f"Headline: {title}\nSummary: {summary[:250]}\n"
            "Respond ONLY with a JSON array: [\"query1\", \"query2\", \"query3\", \"query4\"]"
        )
        try:
            text = await self._call_groq(prompt, max_tokens=100)
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            queries = json.loads(text)
            if isinstance(queries, list):
                return [str(q).strip() for q in queries if str(q).strip()][:5]
            return []
        except Exception as e:
            logger.debug(f"GroqNewsAnalyzer.generate_market_queries error: {e}")
            return []


class GeminiNewsAnalyzer(LLMNewsAnalyzer):
    """
    Uses Google Gemini 2.0 Flash for news classification.
    Free tier: 1,500 requests/day, 1,000,000 tokens/day — ~10x Groq 70B budget.
    Get a free key at: https://aistudio.google.com  (no credit card needed)
    Set GEMINI_API_KEY in your .env file.
    """

    _MODEL = "gemini-2.0-flash"

    def __init__(self):
        NewsAnalyzerBase.__init__(self)
        self._client = None
        self._call_count = 0
        self._last_call_time: float = 0.0
        gemini_key = getattr(settings, "GEMINI_API_KEY", "")
        if not gemini_key or gemini_key in ("", "your-gemini-key"):
            logger.warning("GeminiNewsAnalyzer: GEMINI_API_KEY not set")
            return
        try:
            import google.genai as genai
            self._client = genai.Client(api_key=gemini_key)
            logger.info(f"GeminiNewsAnalyzer: initialized with {self._MODEL} (1M tokens/day free)")
        except ImportError:
            logger.warning("GeminiNewsAnalyzer: google-genai not installed — run: pip install google-genai")
        except Exception as e:
            logger.warning(f"GeminiNewsAnalyzer: init failed: {e}")

    async def _call_gemini(self, prompt: str, max_tokens: int = 200) -> str:
        if self._client is None:
            raise RuntimeError("Gemini client not initialized")
        elapsed = time.time() - self._last_call_time
        if elapsed < 0.5:
            await asyncio.sleep(0.5 - elapsed)
        import google.genai.types as genai_types
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._client.models.generate_content(
                model=self._MODEL,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    temperature=0.1,
                ),
            ),
        )
        self._call_count += 1
        self._last_call_time = time.time()
        return response.text.strip()

    async def analyze(self, news_item, theme: str, theme_config: dict) -> dict:
        if self._client is None:
            return _irrelevant_result(theme, "gemini_flash")
        title = getattr(news_item, "title", "") or ""
        summary = getattr(news_item, "summary", "") or ""
        description = theme_config.get("description", theme)
        prompt = (
            f"You are a prediction market classifier. Analyze this news for theme: {theme}.\n"
            f"Theme description: {description}\n\nHeadline: {title}\nSummary: {summary[:300]}\n\n"
            "Respond ONLY with valid JSON (no markdown, no explanation):\n"
            '{"is_relevant": true or false, "direction": "increase_yes" or "decrease_yes" or "neutral", '
            '"catalyst_class": "official_catalyst" or "confirmed_event" or "policy_action" or "commentary" or "analysis" or "speculation" or "noise", '
            '"confidence": 0.0 to 1.0, "urgency": 0.0 to 1.0, "impact_strength": 0.0 to 1.0, '
            '"reasoning_short": "max 80 chars", "market_tags": ["tag1", "tag2"], '
            '"implied_probability_shift": -0.25 to 0.25}'
        )
        try:
            text = await self._call_gemini(prompt, max_tokens=200)
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            data = json.loads(text)
            is_relevant = bool(data.get("is_relevant", False))
            direction = data.get("direction", "neutral")
            implied_shift = _clamp(float(data.get("implied_probability_shift", 0.0)), -0.25, 0.25)
            if not is_relevant or direction == "neutral":
                implied_shift = 0.0
            elif direction == "increase_yes":
                implied_shift = abs(implied_shift)
            else:
                implied_shift = -abs(implied_shift)
            return {
                "is_relevant": is_relevant,
                "theme": theme,
                "direction": direction,
                "catalyst_class": str(data.get("catalyst_class", "noise")).strip().lower(),
                "confidence": _clamp(float(data.get("confidence", 0.0)), 0.0, 1.0),
                "urgency": _clamp(float(data.get("urgency", 0.0)), 0.0, 1.0),
                "impact_strength": _clamp(float(data.get("impact_strength", 0.0)), 0.0, 1.0),
                "reasoning_short": str(data.get("reasoning_short", ""))[:100],
                "market_tags": data.get("market_tags", []),
                "analyzer_name": "gemini_flash",
                "implied_probability_shift": implied_shift,
            }
        except Exception as e:
            logger.debug(f"GeminiNewsAnalyzer.analyze error: {e}")
            return _irrelevant_result(theme, "gemini_flash")

    async def evaluate_market_pricing(
        self,
        question: str,
        current_price: float,
        hours_remaining: float,
        category: str,
        crypto_prices: dict | None = None,
    ) -> dict:
        if self._client is None:
            return _irrelevant_result(category, "gemini_flash")
        price_context = ""
        if crypto_prices and category in ("crypto_markets", "crypto"):
            from data.crypto_prices import format_prices_for_prompt
            formatted = format_prices_for_prompt(crypto_prices)
            if formatted:
                price_context = (
                    f"\nLIVE CRYPTO PRICES RIGHT NOW: {formatted}\n"
                    "Use these prices to determine if the market question is already decided.\n"
                )
        prompt = (
            f"You are evaluating a prediction market for mispricing.\n"
            f"Market question: {question}\n"
            f"Current market price: {current_price:.3f} ({current_price*100:.1f}% probability YES)\n"
            f"Time until resolution: {hours_remaining:.1f} hours\nCategory: {category}\n"
            f"{price_context}\n"
            "Is this market MISPRICED? Only say yes if you have strong knowledge to assess it.\n"
            "If the market is already near-resolved (price < 0.02 or > 0.98), say is_relevant=false.\n"
            "Respond ONLY with valid JSON:\n"
            '{"is_relevant": true if mispriced else false, "direction": "increase_yes" or "decrease_yes" or "neutral", '
            '"implied_probability_shift": -0.25 to 0.25, "fair_probability": 0.0 to 1.0, '
            '"confidence": 0.0 to 1.0, "reasoning_short": "max 80 chars"}'
        )
        try:
            text = await self._call_gemini(prompt, max_tokens=150)
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            data = json.loads(text)
            return {
                "is_relevant": bool(data.get("is_relevant", False)),
                "direction": data.get("direction", "neutral"),
                "implied_probability_shift": _clamp(float(data.get("implied_probability_shift", 0.0)), -0.25, 0.25),
                "fair_probability": _clamp(float(data.get("fair_probability", current_price)), 0.0, 1.0),
                "confidence": _clamp(float(data.get("confidence", 0.0)), 0.0, 1.0),
                "reasoning_short": str(data.get("reasoning_short", ""))[:100],
                "analyzer_name": "gemini_flash",
            }
        except Exception as e:
            logger.warning(f"GeminiNewsAnalyzer.evaluate_market_pricing error: {e}")
            return _irrelevant_result(category, "gemini_flash")

    async def generate_market_queries(self, news_item) -> list[str]:
        if self._client is None:
            return []
        title = getattr(news_item, "title", "") or ""
        summary = getattr(news_item, "summary", "") or ""
        prompt = (
            f"Generate 4 short Polymarket search queries (2-6 words each) for this headline.\n"
            f"Headline: {title}\nSummary: {summary[:250]}\n"
            "Respond ONLY with a JSON array: [\"query1\", \"query2\", \"query3\", \"query4\"]"
        )
        try:
            text = await self._call_gemini(prompt, max_tokens=100)
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            queries = json.loads(text)
            if isinstance(queries, list):
                return [str(q).strip() for q in queries if str(q).strip()][:5]
            return []
        except Exception as e:
            logger.debug(f"GeminiNewsAnalyzer.generate_market_queries error: {e}")
            return []


def _irrelevant_result(theme: str, analyzer_name: str) -> dict:
    return {
        "is_relevant": False, "theme": theme, "direction": "neutral",
        "catalyst_class": "noise",
        "confidence": 0.0, "urgency": 0.0, "impact_strength": 0.0,
        "reasoning_short": "error or irrelevant", "market_tags": [],
        "analyzer_name": analyzer_name, "implied_probability_shift": 0.0,
    }


def get_analyzer() -> NewsAnalyzerBase:
    analyzer_choice = getattr(settings, "SENTIMENT_ANALYZER", "llm").lower()
    if analyzer_choice == "keyword":
        logger.warning("AISentiment: SENTIMENT_ANALYZER=keyword — using keyword fallback analyzer")
        return KeywordNewsAnalyzer()
    if analyzer_choice == "gemini":
        gemini_key = getattr(settings, "GEMINI_API_KEY", "")
        if gemini_key and gemini_key not in ("", "your-gemini-key"):
            logger.info("AISentiment: using Gemini analyzer (gemini-2.0-flash, 1M tokens/day)")
            return GeminiNewsAnalyzer()
        logger.warning("AISentiment: SENTIMENT_ANALYZER=gemini but GEMINI_API_KEY not set — falling back to Groq")
    if analyzer_choice in ("groq", "gemini"):
        groq_key = getattr(settings, "GROQ_API_KEY", "")
        if groq_key and groq_key not in ("", "your-groq-key"):
            logger.info("AISentiment: using Groq analyzer (Llama 3.3 70B)")
            return GroqNewsAnalyzer()
        logger.warning("AISentiment: GROQ_API_KEY not set — falling back")
    key = getattr(settings, "ANTHROPIC_API_KEY", "")
    if key and key not in ("your-anthropic-key", ""):
        logger.info("AISentiment: using LLM analyzer (Claude Haiku)")
        return LLMNewsAnalyzer()
    logger.warning("AISentiment: no LLM key set — using keyword fallback analyzer")
    return KeywordNewsAnalyzer()
