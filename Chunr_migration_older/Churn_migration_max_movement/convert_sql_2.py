import re
import os


def process_initial_table_3(sql_content, output_directory="processed_sql"):
    """
    Extracts and transforms WHEN conditions from the 'initial_table_3' CTE in an SQL string.

    Args:
        sql_content: The SQL content as a string.
        output_directory: Directory to save the processed SQL file. Defaults to "processed_sql".
    """
    try:
        # Find the 'initial_table_3' CTE
        cte_match = re.search(
            r"initial_table_3\s+as\s+\((.*?)\)", sql_content, re.DOTALL | re.IGNORECASE
        )
        if cte_match:
            cte_content = cte_match.group(1)

            # Find the CASE statement within the CTE using a more robust pattern
            case_match = re.search(
                r"case\s+(.*?)\s+end\s+as\s+\"Movement Classification\"",
                cte_content,
                re.DOTALL | re.IGNORECASE,
            )
            if case_match:
                case_content = case_match.group(1)

                # Extract WHEN conditions
                when_conditions = re.findall(
                    r"when\s+(.*?)\s+then\s+'(.*?)'",
                    case_content,
                    re.DOTALL | re.IGNORECASE,
                )

                # Transform WHEN conditions into new columns
                new_columns = []
                for condition, column_name in when_conditions:
                    transformed_name = re.sub(
                        r"[^a-zA-Z0-9]+", "_", column_name
                    ).lower()
                    new_column = (
                        f"case when {condition} then 1 else 0 end as {transformed_name}"
                    )
                    new_columns.append(new_column)

                # Replace the original CASE statement with the new columns
                full_case_statement = re.search(
                    r"case\s+(.*?)\s+end\s+as\s+\"Movement Classification\"",
                    cte_content,
                    re.DOTALL | re.IGNORECASE,
                ).group(0)
                new_cte_content = cte_content.replace(
                    full_case_statement, ",\n".join(new_columns)
                )
                new_sql_content = sql_content.replace(cte_content, new_cte_content)

                # Create the output directory if it doesn't exist
                os.makedirs(output_directory, exist_ok=True)

                # Create output file name
                output_file_name = "processed_output.sql"
                output_file_path = os.path.join(output_directory, output_file_name)

                with open(output_file_path, "w") as output_file:
                    output_file.write(new_sql_content)

                return f"Processed SQL saved to: {output_file_path}"

            else:
                return "CASE statement not found in 'initial_table_3' CTE."
        else:
            return "'initial_table_3' CTE not found."

    except Exception as e:
        return f"An error occurred: {e}"


# Load the SQL file
file_path = "test.txt"
try:
    with open(file_path, "r") as file:
        sql_content = file.read()
except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
    sql_content = None

# Process and save the modified SQL
if sql_content:
    result = process_initial_table_3(sql_content)
    print(result)
