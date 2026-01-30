"""p3_csv_pipeline.py - CSV ETVL pipeline.

ETVL:
  E = Extract (read)
  T = Transform (process)
  V = Verify (check)
  L = Load (write results to data/processed)

CUSTOM: We turn off some of our PyRight type checks when working with raw data pipelines.
WHY: We don't know what types things are until after we read them.
OBS: See pyproject.toml and the [tool.pyright] section for details.

CUSTOM: We use keyword-only function arguments.
In our functions, you'll see a `*,`.
The asterisk can appear anywhere in the list of parameters.
EVERY argument AFTER the asterisk must be passed
using the named keyword argument (also called kwarg), rather than by position.

WHY: Requiring named arguments prevents argument-order mistakes.
It also makes our function calls self-documenting, which can be especially helpful in
data-processing pipelines.
"""

import csv
from pathlib import Path
import statistics
from typing import Any

# === DEFINE ETL STEP FUNCTIONS ===
# === We add a VERIFY step to check data integrity ===


def extract_csv_orders(
    *, file_path: Path, amount_col: str, state_col: str
) -> tuple[list[float], dict[str, float]]:
    """E: Read CSV and extract order amounts and totals per state.

    Args:
        file_path: Path to input CSV file.
        amount_name: Column of the dollar amounts.
        state_col: Column of the customer's state

    Returns:
        A tuple containing list of dollar amounts, and a dictionnary of states with their total dollar amounts ordered
    """
    # Handle known possible error: no file at the path provided.
    if not file_path.exists():
        raise FileNotFoundError(f"Missing input file: {file_path}")

    amounts: list[float] = []
    state_totals: dict[str, float] = {}

    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Handle known possible error: missing expected column.
        if (
            reader.fieldnames is None
            or (amount_col and state_col) not in reader.fieldnames
        ):
            raise KeyError(
                f"CSV missing expected column '{amount_col}' or '{state_col}'. Found: {reader.fieldnames}"
            )

        for row in reader:
            raw_amount = (row.get(amount_col) or "").strip()
            state = (row.get(state_col) or "").strip()

            if not raw_amount or not state:
                continue
            try:
                amount = float(raw_amount)
            except ValueError:
                # Keep it simple: skip rows that do not convert cleanly.
                continue

            amounts.append(amount)
            state_totals[state] = state_totals[state] + amount

    return amounts, state_totals


def transform_orders_to_stats(*, amounts: list[float]) -> dict[str, float]:
    """T: Calculate basic statistics for a list of floats.

    Args:
        amounts: List of float values.


    Returns:
        Dictionary with keys: count, min, max, mean, stdev.
    """
    if not amounts:
        raise ValueError("No numeric values found for analysis.")

    return {
        "count": float(len(amounts)),
        "min": min(amounts),
        "max": max(amounts),
        "mean": statistics.mean(amounts),
    }


def load_stats_report(
    *, stats: dict[str, float], state_totals: dict[str, float], out_path: Path
) -> None:
    """L: Write stats to a text file in data/processed.

    Args:
        stats: Dictionary with statistics to write.
        out_path: Path to output text file.

    Returns:
        None
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        f.write("Customer Orders Statistics\n")
        f.write("-" * 30 + "\n")

        f.write(f"Count: {int(stats['count'])}\n")
        f.write(f"Minimum: {stats['min']:.2f}\n")
        f.write(f"Maximum: {stats['max']:.2f}\n")
        f.write(f"Mean: {stats['mean']:.2f}\n")

        f.write("Total Order Amount by State\n")
        f.write("-" * 30 + "\n")

        for state, total in sorted(state_totals.items()):
            f.write(f"{state}: ${total:.2f}\n")


# === DEFINE THE FULL PIPELINE FUNCTION ===


def run_csv_pipeline(*, raw_dir: Path, processed_dir: Path, logger: Any) -> None:
    """Run the full ETVL pipeline.

    Args:
        raw_dir: Path to data/raw directory.
        processed_dir: Path to data/processed directory.
        logger: Logger for logging messages.

    Returns:
        None

    """
    logger.info("CSV: START")

    input_file = raw_dir / "customers_orders.csv"
    output_file = processed_dir / "csv_customers_orders_stats.txt"

    # E
    amounts, state_totals = extract_csv_orders(
        file_path=input_file, amount_col="AmountSpentUSD", state_col="State"
    )

    # T
    stats = transform_orders_to_stats(amounts=amounts)

    # L
    load_stats_report(stats=stats, state_totals=state_totals, out_path=output_file)

    logger.info("CSV: wrote %s", output_file)
    logger.info("CSV: END")
