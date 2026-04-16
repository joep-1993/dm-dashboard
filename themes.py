"""
Theme configuration for Thema Ads.

Maps theme keys to their display labels and data directories.
Theme data (headlines.txt, descriptions.txt) lives in themes/<key>/.
"""
import os

THEMES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "themes")

SUPPORTED_THEMES = {
    "sinterklaas": {"label": "Sinterklaas", "dir": os.path.join(THEMES_DIR, "sinterklaas")},
    "kerstmis": {"label": "Kerstmis", "dir": os.path.join(THEMES_DIR, "kerstmis")},
    "black_friday": {"label": "Black Friday", "dir": os.path.join(THEMES_DIR, "black_friday")},
    "cyber_monday": {"label": "Cyber Monday", "dir": os.path.join(THEMES_DIR, "cyber_monday")},
}


def is_valid_theme(theme: str) -> bool:
    return theme in SUPPORTED_THEMES


def get_theme_label(theme: str) -> str:
    info = SUPPORTED_THEMES.get(theme)
    return info["label"] if info else theme


def get_all_theme_labels() -> dict:
    return {key: info["label"] for key, info in SUPPORTED_THEMES.items()}
