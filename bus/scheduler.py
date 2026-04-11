"""Scheduler integration — context-aware model routing.

Selects the appropriate model size based on session context length.
When context grows beyond a model's capacity, recommends stepping up
to a larger model. When context is distilled (compressed), recommends
stepping back down.

This is the bus-side integration point. The actual model management
lives in khonliang-scheduler (Go service). This module provides:

1. Model profile configuration (which models exist, their context limits)
2. Context-size-based model selection
3. Distillation threshold detection (when should context be compressed?)
4. Integration with bus sessions (track context growth per session)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from bus.db import BusDB

logger = logging.getLogger(__name__)


@dataclass
class ModelProfile:
    """Profile for a model available on the platform."""

    name: str
    max_context: int  # tokens
    speed: str = "moderate"  # fast | moderate | slow
    good_for: list[str] = field(default_factory=list)


DEFAULT_PROFILES = [
    ModelProfile("llama3.2:3b", 4096, "fast", ["extraction", "classification", "short tasks"]),
    ModelProfile("qwen2.5:7b", 16384, "moderate", ["summarization", "synthesis", "idea parsing"]),
    ModelProfile("qwen2.5:32b", 65536, "slow", ["review", "complex reasoning", "multi-document analysis"]),
]


class SchedulerIntegration:
    """Context-aware model routing for bus sessions.

    Tracks context size per session and recommends the smallest
    sufficient model. When context hits a threshold, signals that
    distillation should trigger.
    """

    def __init__(
        self,
        db: BusDB,
        profiles: list[ModelProfile] | None = None,
        distill_threshold: float = 0.75,
    ):
        self.db = db
        self.profiles = sorted(profiles or DEFAULT_PROFILES, key=lambda p: p.max_context)
        self.distill_threshold = distill_threshold

    def select_model(self, context_tokens: int) -> ModelProfile:
        """Select the smallest model that fits the context.

        Returns the first model whose ``max_context`` exceeds
        ``context_tokens``. Falls back to the largest model if
        context exceeds all profiles.
        """
        for profile in self.profiles:
            if context_tokens < profile.max_context:
                return profile
        return self.profiles[-1]  # largest available

    def should_distill(self, context_tokens: int, current_model: str = "") -> bool:
        """Check if the current context should trigger distillation.

        Returns True when context hits ``distill_threshold`` of the
        current model's max_context. This is the signal to compress
        context before being forced to step up to a larger model.
        """
        model = self._find_profile(current_model) if current_model else self.select_model(context_tokens)
        threshold = int(model.max_context * self.distill_threshold)
        return context_tokens >= threshold

    def get_session_recommendation(self, session_id: str) -> dict[str, Any]:
        """Get model recommendation for a session based on its context size.

        Reads session context from the bus DB, estimates token count,
        and returns a recommendation.
        """
        session = self.db.get_session(session_id)
        if not session:
            return {"error": "session not found"}

        # Estimate context size from stored context
        public_ctx = session.get("public_ctx", "") or ""
        private_ctx = session.get("private_ctx", "") or ""
        # Rough estimate: 1 token ≈ 4 chars
        total_chars = len(str(public_ctx)) + len(str(private_ctx))
        estimated_tokens = total_chars // 4

        model = self.select_model(estimated_tokens)
        needs_distill = self.should_distill(estimated_tokens, model.name)

        return {
            "session_id": session_id,
            "estimated_tokens": estimated_tokens,
            "recommended_model": model.name,
            "model_max_context": model.max_context,
            "utilization": round(estimated_tokens / model.max_context, 2) if model.max_context else 0,
            "should_distill": needs_distill,
            "distill_threshold": self.distill_threshold,
        }

    def get_model_profiles(self) -> list[dict[str, Any]]:
        """Return all configured model profiles."""
        return [
            {
                "name": p.name,
                "max_context": p.max_context,
                "speed": p.speed,
                "good_for": p.good_for,
            }
            for p in self.profiles
        ]

    def _find_profile(self, model_name: str) -> ModelProfile:
        for p in self.profiles:
            if p.name == model_name:
                return p
        return self.profiles[-1]  # default to largest when name is unknown
