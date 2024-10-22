from reader import ReportBatchReader
import csv

if __name__ == "__main__":
    batch_report_reader = ReportBatchReader("reports", "output")
    batch_report_reader.get_files()
    print(batch_report_reader.files)
    batch_report_reader.get_data()

    output_csv = open("output.csv", "w", newline='')
    
    fields_names = ["COMPANY_NAME", "GUID_FISCAL_PERIOD", "GUIDANCE_LINE_ITEM", "GUID_AMT"]
    writer = csv.DictWriter(output_csv, fieldnames=fields_names)
    writer.writeheader()
    data = []
    
    # Capital Expenditures
    for df in batch_report_reader.summaries:
        df = df[fields_names]
        df = df[df["GUIDANCE_LINE_ITEM"].str.contains("Capital Expenditure")]
        values = df.to_dict("records")
        data.extend(values)

    # Cash Flow
    for df in batch_report_reader.summaries:
        df = df[fields_names]
        df = df[df["GUIDANCE_LINE_ITEM"].str.contains("Cash Flow")]
        values = df.to_dict("records")
        data.extend(values)

    # Earnings
    for df in batch_report_reader.summaries:
        df = df[fields_names]
        df = df[df["GUIDANCE_LINE_ITEM"].str.contains("Earnings")]
        values = df.to_dict("records")
        data.extend(values)

    
    
    writer.writerows(data)
    