import csv
from _csv import QUOTE_MINIMAL
from csv import Dialect
from datetime import datetime
import time

import singer
from sp_api.api import Reports

from tap_amazon_sp import Context
from tap_amazon_sp.streams.report import Report
from tap_amazon_sp.streams.stream import Stream, quota_error_handling

from singer import metrics, utils


class TSVReport(Report):
    report_type: str = ""
    replication_method = "FULL_TABLE"
    replication_key = ""
    key_properties = []

    def sync(self):
        start_date = self.get_bookmark()
        report_id = self.create_report(data_start_time=utils.strftime(start_date, utils.DATETIME_PARSE))
        document_id = self.wait_on_report_finished(report_id)
        self.push_document(document_id)


class OpenListingsDataReport(TSVReport):
    name = "open_listings_data_report"
    report_type = 'GET_FLAT_FILE_OPEN_LISTINGS_DATA'


class MerchantListingsAllDataReport(TSVReport):
    name = "merchant_listings_all_data_report"
    report_type = 'GET_MERCHANT_LISTINGS_ALL_DATA'


class MerchantListingsInactiveDataReport(TSVReport):
    name = "merchant_listings_inactive_data_report"
    report_type = 'GET_MERCHANT_LISTINGS_INACTIVE_DATA'


class MerchantListingsDataBackCompatReport(TSVReport):
    name = "merchant_listings_data_back_compat_report"
    report_type = 'GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT'


class MerchantListingsDataLiteReport(TSVReport):
    name = "merchant_listings_data_lite_report"
    report_type = "GET_MERCHANT_LISTINGS_DATA_LITE"


class AllOrdersDataByOrderDateGeneralReport(TSVReport):
    name = "all_orders_data_by_order_date_general_report"
    report_type = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"


class SellerFeedbackDataReport(TSVReport):
    name = "seller_feedback_data_report"
    report_type = "GET_SELLER_FEEDBACK_DATA"


class AfnInventoryDataReport(TSVReport):
    name = "afn_inventory_data_report"
    report_type = "GET_AFN_INVENTORY_DATA"


class AfnInventoryDataByCountryReport(TSVReport):
    name = "afn_inventory_data_by_country_report"
    report_type = "GET_AFN_INVENTORY_DATA_BY_COUNTRY"


class FbaFulfillmentCurrentInventoryDataReport(TSVReport):
    name = "fba_fulfillment_current_inventory_data_report"
    report_type = "GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA"


class ReservedInventoryDataReport(TSVReport):
    name = "reserved_inventory_data_report"
    report_type = "GET_RESERVED_INVENTORY_DATA"


class FbaFulfillmentInventoryAdjustmentsDataReport(TSVReport):
    name = "fba_fulfillment_inventory_adjustments_data_report"
    report_type = "GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA"


class FbaFulfillmentInventoryHealthDataReport(TSVReport):
    name = "fba_fulfillment_inventory_health_data_report"
    report_type = "GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA"


class FbaMyiUnsuppressedInventoryDataReport(TSVReport):
    name = "fba_myi_unsuppressed_inventory_data_report"
    report_type = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"


class FbaMyiAllInventoryDataReport(TSVReport):
    name = "fba_myi_all_inventory_data_report"
    report_type = "GET_FBA_MYI_ALL_INVENTORY_DATA"


class RestockInventoryRecommendationsReportReport(TSVReport):
    name = "restock_inventory_recommendations_report_report"
    report_type = "GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT"


class FbaInventoryAgedDataReport(TSVReport):
    name = "fba_inventory_aged_data_report"
    report_type = "GET_FBA_INVENTORY_AGED_DATA"


class ExcessInventoryDataReport(TSVReport):
    name = "excess_inventory_data_report"
    report_type = "GET_EXCESS_INVENTORY_DATA"


class FbaStorageFeeChargesDataReport(TSVReport):
    name = "fba_storage_fee_charges_data_report"
    report_type = "GET_FBA_STORAGE_FEE_CHARGES_DATA"


class FbaEstimatedFbaFeesTxtDataReport(TSVReport):
    name = "fba_estimated_fba_fees_txt_data_report"
    report_type = "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA"


class FbaReimbursementsDataReport(TSVReport):
    name = "fba_reimbursements_data_report"
    report_type = "GET_FBA_REIMBURSEMENTS_DATA"


class FbaFulfillmentCustomerReturnsDataReport(TSVReport):
    name = "fba_fulfillment_customer_returns_data_report"
    report_type = "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"


class FbaRecommendedRemovalDataReport(TSVReport):
    name = "fba_recommended_removal_data_report"
    report_type = "GET_FBA_RECOMMENDED_REMOVAL_DATA"
