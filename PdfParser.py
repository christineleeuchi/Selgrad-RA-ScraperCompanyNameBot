from reader import ReportBatchReader, ReportReader
import csv

if __name__ == "__main__":
    batch_report_reader = ReportBatchReader("reports", "output")
    batch_report_reader.get_files()
    print(batch_report_reader.files)

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

    for file in batch_report_reader.files:
        r = ReportReader(file, nocaching=True)
        r.read()
        summary, report, error = r.summary, r.report, r.error
        if summary is None or report is None:
            continue
        file_name = str(file)
        company_name = file_name[file_name.index("/") + 1 :].split(" ")[0]
        report = report.rename(columns={"LAST_ISSUE_DATETIME": "GUID_ISSUE_DATE"})
        report = report.assign(COMPANY_NAME=company_name)
        summary = summary.assign(COMPANY_NAME=company_name)
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
            data = [dict(s) for s in set(frozenset(d.items()) for d in data)]

    writer.writerows(data)
