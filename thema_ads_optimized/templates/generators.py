"""Template generators for headlines and descriptions."""

from typing import List
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from themes import load_theme_content, is_valid_theme


def generate_singles_day_headlines(base_headlines: List[str]) -> List[str]:
    """Generate Singles Day themed headlines."""
    return [
        "{KeyWord: Singles Day Deal}",
        "Singles Day in {COUNTDOWN(2025-11-11 00:00:00,7)}",
        "{KeyWord:Singles Day Deals} Online",
        "Bestel {KeyWord:Vandaag} Met Korting",
        "{KeyWord:Aanbieding} – Gratis Verzending",
        "Top {KeyWord:Acties} – Shop Nu",
        "{KeyWord:Sale} Niet Missen!",
        "Nog {COUNTDOWN(2025-11-11 00:00:00,7)} Tot Singles Day",
        "Singles Day – Nog {COUNTDOWN(2025-11-11 00:00:00,7)}",
        "Shop Nu – Nog {COUNTDOWN(2025-11-11 00:00:00,7)} Te Gaan",
        "Singles Day Start In {COUNTDOWN(2025-11-11 00:00:00,7)}",
        "{KeyWord:Singles Day} – Nog {COUNTDOWN(2025-11-11 00:00:00,7)}",
        "{KeyWord:Aanbieding} Eindigt Over {COUNTDOWN(2025-11-11 00:00:00,7)}",
        "Snel! {KeyWord:Sale} – {COUNTDOWN(2025-11-11 00:00:00,7)} Te Gaan",
        "{KeyWord:Acties} Starten In {COUNTDOWN(2025-11-11 00:00:00,7)}"
    ]


def generate_singles_day_descriptions(base_description: str) -> List[str]:
    """Generate Singles Day themed descriptions."""
    return [
        "{KeyWord:Singles Day Deals} nu met hoge korting. Alleen echte SD deals – geen nepprijzen!",
        "Scoor {KeyWord:Aanbiedingen} tijdens Singles Day. Alleen echte SD deals, op=op!",
        "{KeyWord:Singles Day Deals} nu live. Nog {COUNTDOWN(2025-11-11 00:00:00,7)}! Alleen echte SD deals.",
        "Scoor {KeyWord:Aanbiedingen} voor Singles Day. Slechts {COUNTDOWN(2025-11-11 00:00:00,7)} te gaan – geen nepdeals!"
    ]


def generate_black_friday_headlines(base_headlines: List[str]) -> List[str]:
    """Generate Black Friday themed headlines."""
    return [
        "{KeyWord: Black Friday Deal}",
        "Black Friday in {COUNTDOWN(2025-11-29 00:00:00,7)}",
        "{KeyWord:Black Friday Deals} Online",
        "Bestel {KeyWord:Vandaag} Met Korting",
        "{KeyWord:Aanbieding} – Gratis Verzending",
        "Top {KeyWord:Acties} – Shop Nu",
        "{KeyWord:Sale} Niet Missen!",
        "Nog {COUNTDOWN(2025-11-29 00:00:00,7)} Tot Black Friday",
        "Black Friday – Nog {COUNTDOWN(2025-11-29 00:00:00,7)}",
        "Shop Nu – Nog {COUNTDOWN(2025-11-29 00:00:00,7)} Te Gaan",
        "Black Friday Start In {COUNTDOWN(2025-11-29 00:00:00,7)}",
        "{KeyWord:Black Friday} – Nog {COUNTDOWN(2025-11-29 00:00:00,7)}",
        "{KeyWord:Aanbieding} Eindigt Over {COUNTDOWN(2025-11-29 00:00:00,7)}",
        "Snel! {KeyWord:Sale} – {COUNTDOWN(2025-11-29 00:00:00,7)} Te Gaan",
        "{KeyWord:Acties} Starten In {COUNTDOWN(2025-11-29 00:00:00,7)}"
    ]


def generate_black_friday_descriptions(base_description: str) -> List[str]:
    """Generate Black Friday themed descriptions."""
    return [
        "{KeyWord:Black Friday Deals} nu met hoge korting. Alleen echte BF deals – geen nepprijzen!",
        "Scoor {KeyWord:Aanbiedingen} tijdens Black Friday. Alleen echte BF deals, op=op!",
        "{KeyWord:Black Friday Deals} nu live. Nog {COUNTDOWN(2025-11-29 00:00:00,7)}! Alleen echte BF deals.",
        "Scoor {KeyWord:Aanbiedingen} voor Black Friday. Slechts {COUNTDOWN(2025-11-29 00:00:00,7)} te gaan – geen nepdeals!"
    ]


def generate_themed_content(
    theme: str,
    base_headlines: List[str],
    base_description: str
) -> tuple:
    """Generate themed headlines and descriptions based on theme type.

    Returns:
        (extra_headlines, extra_descriptions, path1)
    """
    theme = theme.lower()

    # Use new theme system if available
    if is_valid_theme(theme):
        try:
            theme_content = load_theme_content(theme)
            return (
                theme_content.headlines,
                theme_content.descriptions,
                theme.replace('_', ' ').lower()
            )
        except Exception as e:
            import logging
            logging.error(f"Failed to load theme content for '{theme}': {e}")
            # Fall back to hardcoded content below

    # Legacy fallback for old themes
    if theme == "singles_day":
        return (
            generate_singles_day_headlines(base_headlines),
            generate_singles_day_descriptions(base_description),
            "singles_day"
        )
    elif theme == "black_friday":
        return (
            generate_black_friday_headlines(base_headlines),
            generate_black_friday_descriptions(base_description),
            "black_friday"
        )
    else:
        # Default/generic - use base content
        return (
            base_headlines,
            [base_description] if base_description else [],
            "deals"
        )
