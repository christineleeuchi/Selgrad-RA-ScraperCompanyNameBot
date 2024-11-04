from reader import ReportBatchReader
import csv

if __name__ == "__main__":
    batch_report_reader = ReportBatchReader("reports", "output")
    batch_report_reader.get_files()
    print(batch_report_reader.files)
    batch_report_reader.get_data()

    output_csv = open("output.csv", "w", newline="")

    fields_names = [
        "COMPANY_NAME",
        "GUID_FISCAL_PERIOD",
        "GUIDANCE_LINE_ITEM",
        "GUID_AMT",
        "GUID_ISSUE_DATE",
    ]
    writer = csv.DictWriter(output_csv, fieldnames=fields_names)
    writer.writeheader()
    data = []

    for i in range(len(batch_report_reader.reports)):
        report = batch_report_reader.reports[i]
        summary = batch_report_reader.summaries[i]
        report = report.rename(columns={"LAST_ISSUE_DATETIME": "GUID_ISSUE_DATE"})
        report = report.assign(COMPANY_NAME=summary["COMPANY_NAME"][0])
        report = report[fields_names]
        summary = summary[fields_names]
        for line_item in ["Capital Expenditure", "Cash Flow", "Earnings"]:
            report_guidance_df = report[
                report["GUIDANCE_LINE_ITEM"].str.contains(line_item)
            ]
            values = report_guidance_df.to_dict("records")
            data.extend(values)
            summary_guidance_df = summary[
                summary["GUIDANCE_LINE_ITEM"].str.contains(line_item)
            ]
            values = summary_guidance_df.to_dict("records")
            data.extend(values)

    writer.writerows(data)
