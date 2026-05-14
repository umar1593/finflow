"""Вспомогательная статистика для consumer."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class RunningStats:
    """Онлайн-оценка среднего и выборочной дисперсии."""

    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float) -> None:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        return self.m2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    def zscore(self, x: float) -> float:
        """Z-оценка наблюдения относительно накопленного распределения."""
        s = self.std
        if s == 0.0:
            return 0.0
        return (x - self.mean) / s


def quantile(sorted_values: list[float], q: float) -> float:
    """Квантиль по линейной интерполяции."""
    n = len(sorted_values)
    if n == 0:
        raise ValueError("пустая выборка")
    if n == 1:
        return sorted_values[0]
    pos = q * (n - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_values[int(pos)]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def iqr_bounds(values: Iterable[float], k: float = 1.5) -> tuple[float, float]:
    """Границы выбросов по межквартильному размаху."""
    data = sorted(values)
    if len(data) < 4:
        return float("-inf"), float("inf")
    q1 = quantile(data, 0.25)
    q3 = quantile(data, 0.75)
    iqr = q3 - q1
    return q1 - k * iqr, q3 + k * iqr


@dataclass
class AnomalyDetector:
    """Потоковый детектор аномальных сумм."""

    z_threshold: float = 3.0
    min_history: int = 5
    per_user: dict[str, RunningStats] = field(default_factory=dict)
    global_stats: RunningStats = field(default_factory=RunningStats)

    def evaluate(self, user_id: str, amount: float) -> dict:
        user = self.per_user.setdefault(user_id, RunningStats())

        z_user = user.zscore(amount) if user.n >= self.min_history else 0.0
        z_global = (
            self.global_stats.zscore(amount)
            if self.global_stats.n >= self.min_history
            else 0.0
        )
        is_anomaly = z_user > self.z_threshold or z_global > self.z_threshold

        user.update(amount)
        self.global_stats.update(amount)

        return {
            "is_anomaly": is_anomaly,
            "z_user": round(z_user, 2),
            "z_global": round(z_global, 2),
        }


@dataclass
class ConfusionMatrix:
    """Счётчики для precision/recall/F1."""

    tp: int = 0
    fp: int = 0
    tn: int = 0
    fn: int = 0

    def update(self, predicted: bool, actual: bool) -> None:
        if predicted and actual:
            self.tp += 1
        elif predicted and not actual:
            self.fp += 1
        elif not predicted and actual:
            self.fn += 1
        else:
            self.tn += 1

    @property
    def precision(self) -> float:
        denom = self.tp + self.fp
        return self.tp / denom if denom else 0.0

    @property
    def recall(self) -> float:
        denom = self.tp + self.fn
        return self.tp / denom if denom else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0
