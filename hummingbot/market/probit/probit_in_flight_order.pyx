from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.market.huobi.huobi_market import HuobiMarket
from hummingbot.market.in_flight_order_base import InFlightOrderBase


cdef class HuobiInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "submitted"):
        super().__init__(
            HuobiMarket,
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state  # submitted, partial-filled, cancelling, filled, canceled, partial-canceled
        )

    @property
    def is_done(self) -> bool:
        raise NotImplementedError("Function is_done not implemented yet for Probit.")
        return self.last_state in {"filled", "canceled", "partial-canceled"}

    @property
    def is_cancelled(self) -> bool:
        raise NotImplementedError("Function is_cancelled not implemented yet for Probit.")
        return self.last_state in {"partial-canceled", "canceled"}

    @property
    def is_failure(self) -> bool:
        raise NotImplementedError("Function is_failure not implemented yet for Probit.")
        return self.last_state in {"canceled"}

    @property
    def is_open(self) -> bool:
        raise NotImplementedError("Function is_open not implemented yet for Probit.")
        return self.last_state in {"submitted", "partial-filled"}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        raise NotImplementedError("Function from_json not implemented yet for Probit.")
        cdef:
            HuobiInFlightOrder retval = HuobiInFlightOrder(
                client_order_id=data["client_order_id"],
                exchange_order_id=data["exchange_order_id"],
                trading_pair=data["trading_pair"],
                order_type=getattr(OrderType, data["order_type"]),
                trade_type=getattr(TradeType, data["trade_type"]),
                price=Decimal(data["price"]),
                amount=Decimal(data["amount"]),
                initial_state=data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval
