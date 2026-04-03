"""Theme management for multi-theme ads system."""

import logging
from pathlib import Path
from typing import List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Theme directory path
THEMES_DIR = Path(__file__).parent.parent / "themes"

# Supported themes
SUPPORTED_THEMES = {
    "black_friday": {
        "label": "THEME_BF",
        "display_name": "Black Friday",
        "countdown_date": "2025-11-28 00:00:00"
    },
    "cyber_monday": {
        "label": "THEME_CM",
        "display_name": "Cyber Monday",
        "countdown_date": "2025-12-01 00:00:00"
    },
    "sinterklaas": {
        "label": "THEME_SK",
        "display_name": "Sinterklaas",
        "countdown_date": "2025-12-05 00:00:00"
    },
    "kerstmis": {
        "label": "THEME_KM",
        "display_name": "Kerstmis",
        "countdown_date": "2025-12-25 00:00:00"
    },
    # Legacy theme for backward compatibility
    "singles_day": {
        "label": "THEME_SD",
        "display_name": "Singles Day",
        "countdown_date": "2025-11-11 00:00:00"
    }
}


@dataclass
class ThemeContent:
    """Theme content container."""
    theme_name: str
    headlines: List[str]
    descriptions: List[str]
    label: str
    display_name: str


def load_theme_content(theme_name: str) -> ThemeContent:
    """Load headlines and descriptions for a specific theme.

    Args:
        theme_name: Name of the theme (e.g., 'black_friday', 'cyber_monday')

    Returns:
        ThemeContent object with headlines, descriptions, and metadata

    Raises:
        ValueError: If theme is not supported or files are missing
    """
    if theme_name not in SUPPORTED_THEMES:
        raise ValueError(
            f"Unsupported theme: {theme_name}. "
            f"Supported themes: {', '.join(SUPPORTED_THEMES.keys())}"
        )

    theme_info = SUPPORTED_THEMES[theme_name]
    theme_dir = THEMES_DIR / theme_name

    # For Singles Day, use legacy file paths (backward compatibility)
    if theme_name == "singles_day":
        # Create legacy content for Singles Day
        headlines = [
            '{KeyWord:Singles Day Deal}',
            'Singles Day in {COUNTDOWN(2025-11-11 00:00:00,5)}',
            '{KeyWord:Singles Day Deals} Online',
            'Bestel {KeyWord:Vandaag}!',
            '{KeyWord:Deal} Shop nu!',
            'Top {KeyWord:Acties} – Shop Nu',
            '{KeyWord:Sale} Niet Missen!',
            'Nog {COUNTDOWN(2025-11-11 00:00:00,5)} Tot Singles Day',
            'Eindigt Over {COUNTDOWN(2025-11-11 00:00:00,5)}',
            'Shop Nu – {COUNTDOWN(2025-11-11 00:00:00,5)} Te Gaan',
            'Singles Day Start In {COUNTDOWN(2025-11-11 00:00:00,5)}',
            '{KeyWord:Singles Day} – Nog {COUNTDOWN(2025-11-11 00:00:00,5)}',
            '{KeyWord:Aanbieding} Nog {COUNTDOWN(2025-11-11 00:00:00,5)}',
            '{KeyWord:Sale} – {COUNTDOWN(2025-11-11 00:00:00,5)}',
            '{KeyWord:Acties} Starten In {COUNTDOWN(2025-11-11 00:00:00,5)}'
        ]

        descriptions = [
            '{KeyWord:Singles Day Deals} nu met hoge korting. Alleen échte SD deals – geen nepprijzen!',
            'Scoor {KeyWord:Aanbiedingen} tijdens Singles Day. Alleen échte SD deals, op=op!',
            '{KeyWord:Singles Day Deals} nu live. Nog {COUNTDOWN(2025-11-11 00:00:00,5)}! Alleen échte SD deals.',
            'Scoor {KeyWord:Aanbiedingen} voor Singles Day. {COUNTDOWN(2025-11-11 00:00:00,5)} te gaan – geen nepdeals!'
        ]
    else:
        # Load from theme files
        headlines_file = theme_dir / "headlines.txt"
        descriptions_file = theme_dir / "descriptions.txt"

        if not headlines_file.exists():
            raise ValueError(f"Headlines file not found for theme '{theme_name}': {headlines_file}")

        if not descriptions_file.exists():
            raise ValueError(f"Descriptions file not found for theme '{theme_name}': {descriptions_file}")

        # Read headlines
        with open(headlines_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            # Parse Python list format (lines with quotes)
            headlines = [line.strip().strip(',') for line in content.split('\n') if line.strip()]

        # Read descriptions
        with open(descriptions_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            descriptions = [line.strip().strip(',') for line in content.split('\n') if line.strip()]

    # Remove quotes from headlines and descriptions if present
    headlines = [h.strip('"').strip("'") for h in headlines if h]
    descriptions = [d.strip('"').strip("'") for d in descriptions if d]

    logger.info(
        f"Loaded theme '{theme_name}': {len(headlines)} headlines, "
        f"{len(descriptions)} descriptions"
    )

    return ThemeContent(
        theme_name=theme_name,
        headlines=headlines,
        descriptions=descriptions,
        label=theme_info["label"],
        display_name=theme_info["display_name"]
    )


def get_theme_label(theme_name: str) -> str:
    """Get the Google Ads label name for a theme.

    Args:
        theme_name: Name of the theme

    Returns:
        Label name (e.g., 'THEME_BF' for Black Friday)
    """
    if theme_name not in SUPPORTED_THEMES:
        raise ValueError(f"Unsupported theme: {theme_name}")

    return SUPPORTED_THEMES[theme_name]["label"]


def get_all_theme_labels() -> List[str]:
    """Get all theme label names.

    Returns:
        List of all theme labels
    """
    return [info["label"] for info in SUPPORTED_THEMES.values()]


def normalize_theme_name(theme_name: str) -> str:
    """Normalize theme name to match supported themes.

    Handles common variations like:
    - "black friday" → "black_friday"
    - "kerst" → "kerstmis"
    - "Black Friday" → "black_friday"

    Args:
        theme_name: Raw theme name from user input

    Returns:
        Normalized theme name, or original if no match found
    """
    # Normalize to lowercase and replace spaces with underscores
    normalized = theme_name.lower().strip().replace(' ', '_')

    # Direct match
    if normalized in SUPPORTED_THEMES:
        return normalized

    # Common aliases
    aliases = {
        'kerst': 'kerstmis',
        'christmas': 'kerstmis',
        'xmas': 'kerstmis',
        'sint': 'sinterklaas',
        'black_friday_2024': 'black_friday',
        'black_friday_2025': 'black_friday',
        'bf': 'black_friday',
        'cm': 'cyber_monday',
        'singles': 'singles_day',
        'sd': 'singles_day',
    }

    if normalized in aliases:
        return aliases[normalized]

    # Return original if no match
    return normalized


def is_valid_theme(theme_name: str) -> bool:
    """Check if a theme name is valid.

    Args:
        theme_name: Name of the theme to check

    Returns:
        True if theme is supported, False otherwise
    """
    normalized = normalize_theme_name(theme_name)
    return normalized in SUPPORTED_THEMES


def get_theme_info(theme_name: str) -> dict:
    """Get information about a theme.

    Args:
        theme_name: Name of the theme

    Returns:
        Dictionary with theme information
    """
    if theme_name not in SUPPORTED_THEMES:
        raise ValueError(f"Unsupported theme: {theme_name}")

    return SUPPORTED_THEMES[theme_name].copy()
