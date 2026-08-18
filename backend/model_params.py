"""Chat-completion parameters per modelfamilie.

WAAROM DIT BESTAAT. Op 2026-08-18 zijn de kopteksten overgezet naar
``gpt-5.6-luna``. Dat is een reasoning-model en die accepteren twee parameters
niet die overal in deze codebase hardcoded stonden:

- ``max_tokens``  -> 400: "Unsupported parameter, use 'max_completion_tokens'"
- ``temperature`` -> 400: "does not support 0.7, only the default (1)"

Reasoning-tokens tellen als output-tokens EN gaan van hetzelfde
completion-budget af. Een budget van 2000 (de oude ``max_tokens``) is daardoor
krap: in de 10-URL benchmark verbruikte Luna gemiddeld 757 output-tokens
waarvan 303 reasoning, met een uitschieter naar 925. Vandaar een ruimer budget
voor reasoning-modellen — je betaalt alleen voor verbruikte tokens, niet voor
het budget.

Gebruik ``chat_params()`` op elk punt waar het model uit een env-var komt, dan
blijft wisselen van model een .env-wijziging in plaats van een codewijziging.
Het resultaat is een gewone dict, dus hij werkt zowel voor
``client.chat.completions.create(**params)`` als voor de ``body`` van een
Batch-API JSONL-regel.
"""
from typing import Dict, Optional

# Modelfamilies die reasoning doen en daarom de afwijkende parameters vragen.
REASONING_PREFIXES = ("gpt-5", "o1", "o3", "o4")

# Ruimer completion-budget voor reasoning-modellen: reasoning-tokens vallen
# hier binnen, dus het oude budget zou de zichtbare tekst kunnen afkappen.
REASONING_BUDGET_MULTIPLIER = 2


def is_reasoning_model(model: str) -> bool:
    return (model or "").startswith(REASONING_PREFIXES)


def chat_params(
    model: str,
    max_output_tokens: int,
    temperature: Optional[float] = 0.7,
    reasoning_effort: Optional[str] = None,
) -> Dict:
    """Bouw de parameters voor één chat-completion, passend bij het model.

    ``max_output_tokens`` is het budget zoals bedoeld voor een klassiek model;
    voor reasoning-modellen wordt het opgehoogd omdat reasoning-tokens er
    binnen vallen.

    ``reasoning_effort`` ("none" | "low" | "medium" | "high") wordt alleen
    meegestuurd voor reasoning-modellen en alleen als hij is opgegeven;
    anders geldt de model-default.
    """
    if is_reasoning_model(model):
        params: Dict = {
            "model": model,
            "max_completion_tokens": max_output_tokens * REASONING_BUDGET_MULTIPLIER,
        }
        if reasoning_effort:
            params["reasoning_effort"] = reasoning_effort
        return params
    params = {"model": model, "max_tokens": max_output_tokens}
    if temperature is not None:
        params["temperature"] = temperature
    return params
