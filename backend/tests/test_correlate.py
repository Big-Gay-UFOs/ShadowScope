"""Placeholder tests for correlation engine."""
import pytest

from backend.correlate import correlate


def test_correlation_placeholder():
    with pytest.raises(NotImplementedError):
        correlate.not_implemented()
