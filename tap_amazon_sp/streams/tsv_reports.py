import csv
from _csv import QUOTE_MINIMAL
from csv import Dialect
from datetime import datetime
import time

import singer
from sp_api.api import Reports

from tap_amazon_sp import Context
from tap_amazon_sp.streams.stream import Stream, quota_error_handling

from singer import metrics, utils


class NewReport(Stream):
    def call_api(self, **kwargs):
        pass

    report_type: str = ""
    replication_method = "FULL_TABLE"
    replication_key = ""
    key_properties = []

    def __init__(self):
        self.report_resource = Reports(marketplace=self.market_place)

    def create_report(self):
        singer.log_info("Creating report with report type: " + self.report_type)
        response = self.report_resource.create_report(dataStartTime=utils.strftime(self.last_executed_date, utils.DATETIME_PARSE), reportType=self.report_type)
        if not response.errors:
            self.report_id = response.payload["reportId"]
            singer.log_info("Successfully created report with id: " + self.report_id)

    def sync(self):
        self.last_executed_date = self.get_bookmark()
        self.create_report()
        self.wait_on_report_finished()
        document_obj = self.report_resource.get_report_document(self.document_id, decrypt=True).payload
        csv_string = document_obj["document"].splitlines()
        clean_data = self.clean_data(csv_string)
        headers = self.clean_headers(clean_data[0])
        yield from csv.DictReader(clean_data[1:], fieldnames=headers, dialect="excel-tab")

    def clean_data(self, data):
        return data

    def clean_headers(self, headers):
        new_headers = []
        for header in headers.split("\t"):
            new_headers.append(header.replace(" ", "_").replace("-", "_").replace("/", "_").replace("\"", "").replace("(", "").replace(")", "").lower())
        return new_headers

    def wait_on_report_finished(self):
        while True:
            time.sleep(15)
            report = self.report_resource.get_report(report_id=self.report_id)
            status = report.payload["processingStatus"]
            if status == "DONE":
                singer.log_info("Report finished processing.")
                self.document_id = report.payload["reportDocumentId"]
                break
            elif status == "PROCESSING":
                singer.log_info("Report is still processing.")
            elif status in ["FATAL", "CANCELLED"]:
                err_message = "Report processing failed with: " + status + " status, please try again later."
                singer.log_warning(err_message)
                raise Exception(err_message)


class OpenListingsDataReport(NewReport):
    name = "open_listings_data_report"
    report_type = 'GET_FLAT_FILE_OPEN_LISTINGS_DATA'


class MerchantListingsAllDataReport(NewReport):
    name = "merchant_listings_all_data_report"
    report_type = 'GET_MERCHANT_LISTINGS_ALL_DATA'


class MerchantListingsInactiveDataReport(NewReport):
    name = "merchant_listings_inactive_data_report"
    report_type = 'GET_MERCHANT_LISTINGS_INACTIVE_DATA'


class MerchantListingsDataBackCompatReport(NewReport):
    name = "merchant_listings_data_back_compat_report"
    report_type = 'GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT'


class MerchantListingsDataLiteReport(NewReport):
    name = "merchant_listings_data_lite_report"
    report_type = "GET_MERCHANT_LISTINGS_DATA_LITE"


class AllOrdersDataByOrderDateGeneralReport(NewReport):
    name = "all_orders_data_by_order_date_general_report"
    report_type = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"


class SellerFeedbackDataReport(NewReport):
    name = "seller_feedback_data_report"
    report_type = "GET_SELLER_FEEDBACK_DATA"


class V2SettlementReportDataReport(NewReport):
    name = "v2_settlement_report_data_report"
    report_type = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"


class V2SettlementReportDataV2Report(NewReport):
    name = "v2_settlement_report_data_v2_report"
    report_type = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"


class AmazonFulfilledShipmentsDataGeneralReport(NewReport):
    name = "amazon_fulfilled_shipments_data_general_report"
    report_type = "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL"


class AfnInventoryDataReport(NewReport):
    name = "afn_inventory_data_report"
    report_type = "GET_AFN_INVENTORY_DATA"


class AfnInventoryDataByCountryReport(NewReport):
    name = "afn_inventory_data_by_country_report"
    report_type = "GET_AFN_INVENTORY_DATA_BY_COUNTRY"


class FbaFulfillmentCurrentInventoryDataReport(NewReport):
    name = "fba_fulfillment_current_inventory_data_report"
    report_type = "GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA"


class ReservedInventoryDataReport(NewReport):
    name = "reserved_inventory_data_report"
    report_type = "GET_RESERVED_INVENTORY_DATA"


class FbaFulfillmentInventoryAdjustmentsDataReport(NewReport):
    name = "fba_fulfillment_inventory_adjustments_data_report"
    report_type = "GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA"


class FbaFulfillmentInventoryHealthDataReport(NewReport):
    name = "fba_fulfillment_inventory_health_data_report"
    report_type = "GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA"


class FbaMyiUnsuppressedInventoryDataReport(NewReport):
    name = "fba_myi_unsuppressed_inventory_data_report"
    report_type = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"


class FbaMyiAllInventoryDataReport(NewReport):
    name = "fba_myi_all_inventory_data_report"
    report_type = "GET_FBA_MYI_ALL_INVENTORY_DATA"


class RestockInventoryRecommendationsReportReport(NewReport):
    name = "restock_inventory_recommendations_report_report"
    report_type = "GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT"


class FbaInventoryAgedDataReport(NewReport):
    name = "fba_inventory_aged_data_report"
    report_type = "GET_FBA_INVENTORY_AGED_DATA"


class ExcessInventoryDataReport(NewReport):
    name = "excess_inventory_data_report"
    report_type = "GET_EXCESS_INVENTORY_DATA"


class FbaStorageFeeChargesDataReport(NewReport):
    name = "fba_storage_fee_charges_data_report"
    report_type = "GET_FBA_STORAGE_FEE_CHARGES_DATA"


class FbaEstimatedFbaFeesTxtDataReport(NewReport):
    name = "fba_estimated_fba_fees_txt_data_report"
    report_type = "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA"


class FbaReimbursementsDataReport(NewReport):
    name = "fba_reimbursements_data_report"
    report_type = "GET_FBA_REIMBURSEMENTS_DATA"


class FbaFulfillmentCustomerReturnsDataReport(NewReport):
    name = "fba_fulfillment_customer_returns_data_report"
    report_type = "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"


class FbaRecommendedRemovalDataReport(NewReport):
    name = "fba_recommended_removal_data_report"
    report_type = "GET_FBA_RECOMMENDED_REMOVAL_DATA"
