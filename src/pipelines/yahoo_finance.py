from __future__ import annotations

from typing import Any

import pandas as pd
import yfinance as yf

from src.pipelines.base import BasePipeline


class YahooFinancePipeline(BasePipeline):
    """Pipeline for Yahoo Finance data."""

    def __init__(
        self,
        symbols: list[str] | None = None,
        period: str = "1mo",
        interval: str = "1d",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize Yahoo Finance pipeline.

        Args:
            symbols: List of stock symbols to fetch (e.g., ['AAPL', 'GOOGL', 'MSFT'])
                    If None, uses default list of popular stocks
            period: Valid periods: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
            interval: Valid intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
            *args: Additional positional arguments for BasePipeline
            **kwargs: Additional keyword arguments for BasePipeline
        """
        super().__init__("yahoo_finance", *args, **kwargs)
        self.symbols = symbols or ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        self.period = period
        self.interval = interval
        # Note: yfinance does not require an API key

    def extract(self) -> pd.DataFrame:
        """Extract stock market data from Yahoo Finance.

        Fetches historical stock data for the specified symbols.

        Returns:
            DataFrame with columns: Date, Symbol, Open, High, Low, Close, Volume, etc.

        Raises:
            Exception: If data extraction fails
        """
        all_data: list[pd.DataFrame] = []

        for symbol in self.symbols:
            try:
                self.logger.debug(
                    "yahoo_finance_fetching_symbol",
                    symbol=symbol,
                    period=self.period,
                    interval=self.interval,
                )

                ticker = yf.Ticker(symbol)
                df = ticker.history(period=self.period, interval=self.interval)

                if df.empty:
                    self.logger.warning(
                        "yahoo_finance_no_data",
                        symbol=symbol,
                    )
                    continue

                # Reset index to make Date a column
                df = df.reset_index()
                df["Symbol"] = symbol

                # Reorder columns to have Symbol early
                cols = ["Date", "Symbol"] + [c for c in df.columns if c not in ["Date", "Symbol"]]
                df = df[cols]

                all_data.append(df)

                self.logger.info(
                    "yahoo_finance_symbol_fetched",
                    symbol=symbol,
                    rows=len(df),
                    columns=list(df.columns),
                )

            except Exception as e:
                self.logger.error(
                    "yahoo_finance_symbol_failed",
                    symbol=symbol,
                    error=str(e),
                    exc_info=True,
                )
                # Continue with other symbols instead of failing completely
                continue

        if not all_data:
            raise ValueError("No data extracted for any symbol")

        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)

        self.logger.info(
            "yahoo_finance_extraction_successful",
            symbols=self.symbols,
            total_rows=len(combined_df),
            symbols_fetched=len(all_data),
        )

        return combined_df
