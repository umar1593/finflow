"""
FinFlow — статистический модуль обнаружения аномалий.

Содержит онлайн-оценку среднего и дисперсии (алгоритм Уэлфорда),
детекторы выбросов (Z-оценка и межквартильный размах Тьюки) и
матрицу ошибок для оценки качества детектора.

Модуль не зависит от Kafka и Postgres — он чистый и легко тестируется
(см. tests/test_stats.py). Kafka consumer импортирует его и применяет
к потоку транзакций.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class RunningStats:
    """Онлайн-оценка среднего и выборочной дисперсии (алгоритм Уэлфорда, 1962).

    Вычисляет mean и variance за один проход по потоку, используя O(1)
    памяти. В отличие от наивной формулы через sum(x) и sum(x^2),
    алгоритм Уэлфорда численно устойчив и не теряет точность на
    больших объёмах данных.
    """

    n: int = 0
    mean: float = 0.0
    m2: float = 0.0  # накопленная сумма квадратов отклонений от среднего

    def update(self, x: float) -> None:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        """Несмещённая выборочная дисперсия (делитель n-1)."""
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
    """Квантиль уровня q по линейной интерполяции (метод по умолчанию в numpy)."""
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
    """Границы выбросов по межквартильному размаху (метод Тьюки).

    Возвращает (lower, upper); значения вне этого интервала считаются
    выбросами. k=1.5 — классический порог, k=3.0 — «экстремальные»
    выбросы. Метод устойчив к самим выбросам, т.к. опирается на
    квартили, а не на среднее.
    """
    data = sorted(values)
    if len(data) < 4:
        return float("-inf"), float("inf")
    q1 = quantile(data, 0.25)
    q3 = quantile(data, 0.75)
    iqr = q3 - q1
    return q1 - k * iqr, q3 + k * iqr


@dataclass
class AnomalyDetector:
    """Потоковый детектор аномальных транзакций.

    Транзакция помечается подозрительной, если её сумма даёт Z-оценку
    выше порога — либо относительно истории конкретного пользователя,
    либо относительно глобального распределения сумм.

    Важно: метка is_fraud из генератора при принятии решения НЕ
    используется. Детектор работает «вслепую», а is_fraud служит лишь
    для последующей оценки качества (precision / recall / F1).
    """

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

        # статистику обновляем ПОСЛЕ оценки, чтобы выброс не «размывал»
        # собственную базовую линию ещё до того, как его заметили
        user.update(amount)
        self.global_stats.update(amount)

        return {
            "is_anomaly": is_anomaly,
            "z_user": round(z_user, 2),
            "z_global": round(z_global, 2),
        }


@dataclass
class ConfusionMatrix:
    """Матрица ошибок: сравнивает предсказание детектора с меткой is_fraud."""

    tp: int = 0  # детектор сказал «аномалия» и это фрод
    fp: int = 0  # детектор сказал «аномалия», но фрода не было
    tn: int = 0
    fn: int = 0  # детектор пропустил реальный фрод

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
