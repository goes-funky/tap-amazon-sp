from tap_amazon_sp.streams.report import Report


class TransactionReport(Report):
    report_type = 'GET_DATE_RANGE_FINANCIAL_TRANSACTION_DATA'
    name = "transactions_report"
    api_access_key = None
    replication_method = "FULL_IMPORT"

    def clean_data(self, data):
        return data[7:]
