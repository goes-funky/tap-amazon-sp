from tap_amazon_sp.streams.orders_metrics_hourly import OrdersMetricsHourly
from tap_amazon_sp.streams.orders import OrdersData
from tap_amazon_sp.streams.order_items import OrderItems
from tap_amazon_sp.streams.transaction_report import TransactionReport

models = [OrdersData, OrderItems, TransactionReport]
