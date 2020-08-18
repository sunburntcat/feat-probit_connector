#!/usr/bin/env python

import aiohttp
import asyncio
import gzip
import json
import logging
import pandas as pd
import time
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.market.huobi.huobi_order_book import HuobiOrderBook

PROBIT_SYMBOLS_URL = "https://api.probit.com/api/exchange/v1/market"
PROBIT_TICKER_URL = "https://api.probit.com/api/exchange/v1/ticker"
PROBIT_DEPTH_URL = "https://api.probit.com/api/exchange/v1/order_book"
PROBIT_WS_URI = ""  # Ignoring WS data connector for now


class HuobiAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _haobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._haobds_logger is None:
            cls._haobds_logger = logging.getLogger(__name__)
        return cls._haobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading pair as index and include usd volume, baseAsset and quoteAsset
        """
        #raise NotImplementedError("Function get_active_exchange_markets not implemented yet for Probit.")
        async with aiohttp.ClientSession() as client:

            market_response, exchange_response = await safe_gather(
                client.get(PROBIT_TICKER_URL),
                client.get(PROBIT_SYMBOLS_URL)
            )
            market_response: aiohttp.ClientResponse = market_response
            exchange_response: aiohttp.ClientResponse = exchange_response

            if market_response.status != 200:
                raise IOError(f"Error fetching Probit markets information. "
                              f"HTTP status is {market_response.status}.")
            if exchange_response.status != 200:
                raise IOError(f"Error fetching Probit exchange information. "
                              f"HTTP status is {exchange_response.status}.")

            market_data = await market_response.json()
            exchange_data = await exchange_response.json()

            attr_name_map = {"base_currency_id": "baseAsset", "quote_currency_id": "quoteAsset"}

            trading_pairs: Dict[str, Any] = {
                item["id"]: {attr_name_map[k]: item[k] for k in ["base_currency_id", "quote_currency_id"]}
                for item in exchange_data["data"]
                #if item["state"] == "online" # Not relevant for Probit
            }

            market_data: List[Dict[str, Any]] = [
                {**item, **trading_pairs[item["market_id"]]}
                for item in market_data["data"]
                if item["market_id"] in trading_pairs
            ]

            # Build the data frame.
            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="market_id")
            all_markets.loc[:, "USDVolume"] = all_markets.quote_volume
            all_markets.loc[:, "volume"] = all_markets.base_volume

            return all_markets.sort_values("USDVolume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        raise NotImplementedError("Function get_trading_pairs not implemented yet for Probit.")

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        raise NotImplementedError("Function get_snapshot not implemented yet for Probit.")

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        raise NotImplementedError("Function get_tracking_pairs not implemented yet for Probit.")

    async def _inner_messages(self,
                                ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
                                # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        raise NotImplementedError("Function _inner_messages not implemented yet for Probit.")

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        raise NotImplementedError("Function listen_for_trades not implemented yet for Probit.")

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        raise NotImplementedError("Function listen_for_order_book_diffs not implemented yet for Probit.")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        raise NotImplementedError("Function listen_for_order_book_snapshots not implemented yet for Probit.")
