#!/usr/bin/env python
from decimal import Decimal

from aiokafka import ConsumerRecord
import bz2
import logging
from sqlalchemy.engine import RowProxy
from typing import (
    Any,
    Optional,
    Dict
)
import ujson

from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

_hob_logger = None


cdef class HuobiOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _hob_logger
        if _hob_logger is None:
            _hob_logger = logging.getLogger(__name__)
        return _hob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, Any],
                                       timestamp: Optional[float] = None,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function snapshot_message_from_exchange not implemented yet for Probit.")
        if metadata:
            msg.update(metadata)
        bids = []
        asks = []
        # Probit order book response doesn't separate bids and asks
        for entry in msg["data"]:
            if entry["side"] == 'buy':
                bids.append([entry["price"], entry["quantity"]])
            elif entry["side"] == 'sell'
                asks.append([entry["price"], entry["quantity"]])

        content = {
            "trading_pair": msg["market_id"],
            "update_id": timestamp * 1e-3,
            "bids": bids
            "asks": asks
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp)

    @classmethod
    def trade_message_from_exchange(cls,
                                   msg: Dict[str, Any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function trade_message_from_exchange not implemented yet for Probit.")
        if metadata:
            msg.update(metadata)
        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.SELL.value) if msg["direction"] == "buy" else float(TradeType.BUY.value),
            "trade_id": msg["id"],
            "update_id": msg_ts,
            "amount": msg["amount"],
            "price": msg["price"]
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, Any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function diff_message_from_exchange not implemented yet for Probit.")
        if metadata:
            msg.update(metadata)
        content = {
            "trading_pair": msg["ch"].split(".")[1],
            "update_id": msg_ts,
            "bids": msg["tick"]["bids"],
            "asks": msg["tick"]["asks"]
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function snapshot_message_from_db not implemented yet for Probit.")
        ts = record["timestamp"]
        msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["ch"].split(".")[1],
            "update_id": int(ts),
            "bids": msg["tick"]["bids"],
            "asks": msg["tick"]["asks"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function diff_message_from_db not implemented yet for Probit.")
        ts = record["timestamp"]
        msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["s"],
            "update_id": int(ts),
            "bids": msg["b"],
            "asks": msg["a"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function snapshot_message_from_kafka not implemented yet for Probit.")
        ts = record.timestamp
        msg = ujson.loads(record.value.decode())
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["ch"].split(".")[1],
            "update_id": ts,
            "bids": msg["tick"]["bids"],
            "asks": msg["tick"]["asks"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        raise NotImplementedError("Function diff_message_from_kafka not implemented yet for Probit.")
        decompressed = bz2.decompress(record.value)
        msg = ujson.loads(decompressed)
        ts = record.timestamp
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["s"],
            "update_id": ts,
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        raise NotImplementedError("Function trade_message_from_db not implemented yet for Probit.")
        msg = record["json"]
        ts = record.timestamp
        data = msg["tick"]["data"][0]
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["ch"].split(".")[1],
            "trade_type": float(TradeType.BUY.value) if data["direction"] == "sell"
                            else float(TradeType.SELL.value),
            "trade_id": ts,
            "update_id": ts,
            "price": data["price"],
            "amount": data["amount"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        raise NotImplementedError("Function from_snapshot not implemented yet for Probit.")
        retval = HuobiOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval
