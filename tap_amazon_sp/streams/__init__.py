from tap_amazon_sp.streams.tsv_reports import OpenListingsDataReport, MerchantListingsAllDataReport, MerchantListingsInactiveDataReport, MerchantListingsDataBackCompatReport, MerchantListingsDataLiteReport, AllOrdersDataByOrderDateGeneralReport, SellerFeedbackDataReport, V2SettlementReportDataReport, V2SettlementReportDataV2Report, AmazonFulfilledShipmentsDataGeneralReport, AfnInventoryDataReport, AfnInventoryDataByCountryReport, FbaFulfillmentCurrentInventoryDataReport, \
    ReservedInventoryDataReport, FbaFulfillmentInventoryAdjustmentsDataReport, FbaFulfillmentInventoryHealthDataReport, FbaMyiUnsuppressedInventoryDataReport, FbaMyiAllInventoryDataReport, RestockInventoryRecommendationsReportReport, FbaInventoryAgedDataReport, ExcessInventoryDataReport, FbaStorageFeeChargesDataReport, FbaEstimatedFbaFeesTxtDataReport, FbaReimbursementsDataReport, FbaFulfillmentCustomerReturnsDataReport, FbaRecommendedRemovalDataReport
from tap_amazon_sp.streams.orders_metrics_hourly import OrdersMetricsHourly
from tap_amazon_sp.streams.orders import OrdersData
from tap_amazon_sp.streams.order_items import OrderItems
from tap_amazon_sp.streams.transaction_report import TransactionReport

models = [
    OrdersData,
    OrderItems,
    TransactionReport,

    OpenListingsDataReport,
    MerchantListingsAllDataReport,
    MerchantListingsInactiveDataReport,
    MerchantListingsDataBackCompatReport,
    MerchantListingsDataLiteReport,
    AllOrdersDataByOrderDateGeneralReport,
    SellerFeedbackDataReport,
    V2SettlementReportDataReport,
    V2SettlementReportDataV2Report,
    AmazonFulfilledShipmentsDataGeneralReport,
    AfnInventoryDataReport,
    AfnInventoryDataByCountryReport,
    FbaFulfillmentCurrentInventoryDataReport,
    ReservedInventoryDataReport,
    FbaFulfillmentInventoryAdjustmentsDataReport,
    FbaFulfillmentInventoryHealthDataReport,
    FbaMyiUnsuppressedInventoryDataReport,
    FbaMyiAllInventoryDataReport,
    RestockInventoryRecommendationsReportReport,
    FbaInventoryAgedDataReport,
    ExcessInventoryDataReport,
    FbaStorageFeeChargesDataReport,
    FbaEstimatedFbaFeesTxtDataReport,
    FbaReimbursementsDataReport,
    FbaFulfillmentCustomerReturnsDataReport,
    FbaRecommendedRemovalDataReport,
]
