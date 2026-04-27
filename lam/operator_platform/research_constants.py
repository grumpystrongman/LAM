from __future__ import annotations

from typing import Dict, List

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

RECOMMENDATION_RESEARCH_TOKENS = {
    "best",
    "recommend",
    "which one",
    "what should i buy",
    "to buy",
    "for dinner",
    "pairing",
    "compare",
}

QUERY_NOISE_TERMS = {
    "best",
    "better",
    "top",
    "recommend",
    "recommended",
    "recommendation",
    "buy",
    "buying",
    "guide",
    "review",
    "reviews",
    "compare",
    "comparison",
    "options",
    "option",
    "find",
    "research",
    "search",
    "look",
    "lookup",
    "please",
    "help",
    "which",
    "what",
    "should",
    "would",
    "could",
    "with",
    "from",
    "there",
    "then",
    "one",
    "the",
    "and",
    "for",
    "tonight",
    "under",
    "dollar",
    "dollars",
}

WINE_STYLE_KEYWORDS: Dict[str, List[str]] = {
    "Cabernet Sauvignon": ["cabernet sauvignon", "cabernet", "cab sauv"],
    "Malbec": ["malbec"],
    "Syrah / Shiraz": ["syrah", "shiraz"],
    "Merlot": ["merlot"],
    "Zinfandel": ["zinfandel", "zin"],
    "Pinot Noir": ["pinot noir", "pinot"],
}

STEAK_WINE_STYLE_BONUS = {
    "Cabernet Sauvignon": 3.0,
    "Malbec": 2.8,
    "Syrah / Shiraz": 2.4,
    "Zinfandel": 1.8,
    "Merlot": 1.6,
}
