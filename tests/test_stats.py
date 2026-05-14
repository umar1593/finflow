"""Проверки статистических функций."""
import statistics

import pytest

from stats import (
    RunningStats,
    quantile,
    iqr_bounds,
    AnomalyDetector,
    ConfusionMatrix,
)


def test_running_stats_matches_stdlib():
    data = [1.0, 2.0, 3.0, 4.0, 5.0, 100.0, 7.0, 8.0]
    rs = RunningStats()
    for x in data:
        rs.update(x)

    assert rs.n == len(data)
    assert rs.mean == pytest.approx(statistics.mean(data))
    assert rs.variance == pytest.approx(statistics.variance(data))
    assert rs.std == pytest.approx(statistics.stdev(data))


def test_running_stats_empty_and_single():
    rs = RunningStats()
    assert rs.variance == 0.0
    assert rs.std == 0.0
    assert rs.zscore(10) == 0.0

    rs.update(5.0)
    assert rs.mean == 5.0
    assert rs.variance == 0.0
    assert rs.zscore(5.0) == 0.0


def test_running_stats_zscore():
    rs = RunningStats()
    for x in [10, 10, 10, 10, 20, 10, 10, 10]:
        rs.update(x)
    assert rs.zscore(20) > 1.5
    assert rs.zscore(10) < 0


def test_quantile_linear_interpolation():
    data = [1, 2, 3, 4]
    assert quantile(data, 0.0) == 1
    assert quantile(data, 1.0) == 4
    assert quantile(data, 0.5) == pytest.approx(2.5)


def test_iqr_bounds_flags_outlier():
    data = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    lower, upper = iqr_bounds(data, k=1.5)
    assert lower < 10
    assert upper > 19
    assert 1000 > upper


def test_iqr_bounds_small_sample_is_permissive():
    lower, upper = iqr_bounds([1, 2], k=1.5)
    assert lower == float("-inf")
    assert upper == float("inf")


def test_detector_ignores_until_min_history():
    det = AnomalyDetector(z_threshold=3.0, min_history=5)
    for _ in range(4):
        res = det.evaluate("user-1", 10.0)
        assert res["is_anomaly"] is False


def test_detector_flags_clear_outlier():
    det = AnomalyDetector(z_threshold=3.0, min_history=5)
    history = [10, 12, 9, 11, 13, 8, 10, 12, 9, 11,
               10, 14, 9, 12, 11, 10, 13, 8, 12, 10]
    for x in history:
        det.evaluate("user-1", float(x))
    res = det.evaluate("user-1", 5000.0)
    assert res["is_anomaly"] is True
    assert res["z_user"] > 3.0


def test_detector_keeps_users_independent():
    det = AnomalyDetector(z_threshold=3.0, min_history=5)
    for _ in range(20):
        det.evaluate("user-1", 10.0)
    res = det.evaluate("user-2", 5000.0)
    assert res["is_anomaly"] is False


def test_confusion_matrix_counts_and_metrics():
    cm = ConfusionMatrix()
    cm.update(predicted=True, actual=True)
    cm.update(predicted=True, actual=True)
    cm.update(predicted=True, actual=False)
    cm.update(predicted=False, actual=True)
    cm.update(predicted=False, actual=False)

    assert (cm.tp, cm.fp, cm.fn, cm.tn) == (2, 1, 1, 1)
    assert cm.precision == pytest.approx(2 / 3)
    assert cm.recall == pytest.approx(2 / 3)
    assert cm.f1 == pytest.approx(2 / 3)


def test_confusion_matrix_zero_division_safe():
    cm = ConfusionMatrix()
    assert cm.precision == 0.0
    assert cm.recall == 0.0
    assert cm.f1 == 0.0
