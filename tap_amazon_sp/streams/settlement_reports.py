import csv
import time

import singer
from sp_api.api import Reports
from sp_api.base import Marketplaces

from tap_amazon_sp import Context
from tap_amazon_sp.streams.report import Report
from tap_amazon_sp.streams.stream import Stream, quota_error_handling
from tap_amazon_sp.streams.tsv_reports import TSVReport

"""
Settlement reports cannot be requested or scheduled. They are automatically scheduled by Amazon. You can search for these reports using the getReports operation.
"""


class SettlementReport(Report):
    name = ""
    report_type = ""
    replication_method = "INCREMENTAL"
    replication_key = ""
    key_properties = []
    documents = []

    def fill_all_report_document(self):
        response = self.client_wrapper(self.report_resource.get_reports, reportTypes=[self.report_type])
        self.documents = response.payload
        while response.next_token:
            time.sleep(5)
            response = self.client_wrapper(self.report_resource.get_reports, nextToken=response.next_token)
            self.documents = self.documents + response.payload

    def sync(self):
        last_imported_report_id = self.get_bookmark(bookmark_key="last_imported_report_id")
        self.fill_all_report_document()
        documents_to_import = []
        if last_imported_report_id:
            for document in self.documents:
                if document["reportId"] == last_imported_report_id:
                    break
                documents_to_import.append(document)
        else:
            documents_to_import = self.documents

        for document in reversed(documents_to_import):  # from oldest to newest
            if document["processingStatus"] == "DONE":
                singer.log_info("Pushing data for date range: " + document["dataStartTime"] + " ==> " + document["dataEndTime"])
                yield from self.push_document(document["reportDocumentId"])
                last_imported_report_id = document["reportId"]

        self.update_bookmark(last_imported_report_id, "last_imported_report_id")






class V2SettlementReportDataV2Report(SettlementReport):
    name = "v2_settlement_report_data_v2_report"
    report_type = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"


class V2SettlementReportDataReport(SettlementReport):
    name = "v2_settlement_report_data_report"
    report_type = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"
