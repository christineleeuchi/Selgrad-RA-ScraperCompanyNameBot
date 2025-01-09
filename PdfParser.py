from reader import ReportReader
import csv, os

if __name__ == "__main__":
    current_dir = os.getcwd()
    relative_dir = "reports"
    full_path = os.path.join(current_dir, relative_dir)
    files = os.listdir(full_path)
    print(files)

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

    for file in files:
        report_reader = ReportReader(relative_dir + "/" + file, nocaching=True)
        report_reader.reset()
        report_reader.read()
        summary, report, error = (
            report_reader.summary,
            report_reader.report,
            report_reader.error,
        )
        if summary is None or report is None:
            continue
        file_name = str(file)
        print(file_name)
        company_name = file_name.split(" ")[0]
        report = report.rename(columns={"LAST_ISSUE_DATETIME": "GUID_ISSUE_DATE"})
        report = report.assign(COMPANY_NAME=company_name)
        summary = summary.assign(COMPANY_NAME=company_name)
        report = report[fields_names]
        summary = summary[fields_names]
        for line_item in ["Capital Expenditure", "Cash Flow", "Earnings"]:
            report_guidance_df = report[
                report["GUIDANCE_LINE_ITEM"].str.contains(line_item)
            ]
            report_records = report_guidance_df.to_dict("records")
            data.extend(report_records)
            summary_guidance_df = summary[
                summary["GUIDANCE_LINE_ITEM"].str.contains(line_item)
            ]
            summary_records = summary_guidance_df.to_dict("records")
            data.extend(summary_records)
            data = [dict(s) for s in set(frozenset(d.items()) for d in data)]

    writer.writerows(data)
