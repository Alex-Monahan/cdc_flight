"""Small commit-log value projections kept outside the applier protocol."""

from datetime import UTC, datetime


def epoch_ms(value):
    """Convert Debezium's millisecond source timestamp to an aware datetime."""
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=UTC)
