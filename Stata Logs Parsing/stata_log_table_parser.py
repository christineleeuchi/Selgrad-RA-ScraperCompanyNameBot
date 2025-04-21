import csv

if __name__ == "__main__":
    log_file = open("4.insurer_holdings - DE.log")
    log_lines = log_file.readlines()
    is_currently_parsing_table = False
    current_table_data = []
    current_table_primary_key = None
    appending_to_row_mode = False
    appending_to_row_index = None
    csv_file_number = 0

    def write_out_table_data():
        global current_table_data, is_currently_parsing_table, current_table_primary_key, appending_to_row_mode, appending_to_row_index, csv_file_number
        # Open the file in write mode
        with open(
            f"stata_tables/estpost_table_{csv_file_number}.csv", "w", newline=""
        ) as csv_file:
            # Create a CSV writer object
            writer = csv.writer(csv_file)
            # Write the data rows
            writer.writerows(current_table_data)

        current_table_data = []
        current_table_primary_key = None
        is_currently_parsing_table = False
        appending_to_row_mode = False
        appending_to_row_index = None
        csv_file_number += 1

    for line in log_lines:
        if "estpost" in line:
            if is_currently_parsing_table:
                write_out_table_data()
            is_currently_parsing_table = True
        elif is_currently_parsing_table:
            if "|" in line:
                line_data = line.split("|")
                if len(current_table_data) == 0:
                    current_table_primary_key = line_data[0].split() or [""]
                    current_table_data.append(
                        current_table_primary_key + line_data[1].split()
                    )
                else:
                    table_primary_key = line_data[0].split() or [""]
                    if table_primary_key == current_table_primary_key:
                        current_table_data[0] += line_data[1].split()
                        appending_to_row_mode = True
                        appending_to_row_index = 1
                    elif appending_to_row_mode:
                        current_table_data[appending_to_row_index] += line_data[
                            1
                        ].split()
                        appending_to_row_index += 1
                    else:
                        current_table_data.append(
                            line_data[0].split() + line_data[1].split()
                        )
            elif line == "\n" or "-+-" in line:
                continue
            elif len(current_table_data) > 0:
                write_out_table_data()

