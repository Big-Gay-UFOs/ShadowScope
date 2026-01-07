"""Placeholder tests for parser modules."""
import pytest

from backend.parsers import pdf_text


def test_pdf_parser_placeholder():
    with pytest.raises(NotImplementedError):
        pdf_text.extract_text_from_pdf("dummy.pdf")
