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

HUOBI_SYMBOLS_URL = "https://api.huobi.pro/v1/common/symbols"
HUOBI_TICKER_URL = "https://api.huobi.pro/market/tickers"
HUOBI_DEPTH_URL = "https://api.huobi.pro/market/depth"
HUOBI_WS_URI = "wss://api.huobi.pro/ws"


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
        raise NotImplementedError("Function get_active_exchange_markets not implemented yet for Probit.")

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
