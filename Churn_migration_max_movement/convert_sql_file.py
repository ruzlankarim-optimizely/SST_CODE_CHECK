import re


def format_column_name(case_output):
    """Convert case output string to snake_case column name."""
    return (
        case_output.lower()
        .replace(" - ", "_")
        .replace(" ", "_")
        .replace("--", "")
        .replace("-", "_")
    )


def extract_cte(sql_text, cte_name):
    """Extract the CTE block for modification."""
    pattern = rf"({cte_name}\s+as\s+\(\s*select)(.*?)(\)\s*,?)"
    match = re.search(pattern, sql_text, re.DOTALL | re.IGNORECASE)
    return match if match else None


def process_case_statements(case_block):
    """Process the case statements inside the CTE."""
    case_statements = []

    # Remove commented lines
    lines = [
        line for line in case_block.split("\n") if not line.strip().startswith("--")
    ]
    print(lines[0])
    case_pattern = re.compile(r"when (.*?) then '(.*?)'", re.IGNORECASE)

    for line in lines:
        match = case_pattern.search(line)
        if match:
            condition, case_output = match.groups()
            column_name = format_column_name(case_output)
            new_case = f"    case when {condition} then 1 end as {column_name}"
            case_statements.append(new_case)

    return "\n".join(case_statements)


def modify_sql_file(sql_file_path):
    """Read, modify, and return the updated SQL file."""
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_text = file.read()

    cte_name = "initial_table_3"
    cte_match = extract_cte(sql_text, cte_name)

    if not cte_match:
        raise ValueError(f"CTE '{cte_name}' not found in the SQL file.")

    cte_start, case_block, cte_end = cte_match.groups()

    # Extract and transform case statements
    modified_case_statements = process_case_statements(case_block)
    print(modified_case_statements)
    # Reconstruct the SQL with modified CTE
    modified_cte = f"{cte_start}\n{modified_case_statements}\n{cte_end}"
    updated_sql = sql_text.replace(cte_match.group(0), modified_cte)

    return updated_sql


# Example usage:
sql_file_path = "cleaned_churn_migration_classifier.sql"
updated_sql_text = modify_sql_file(sql_file_path)

# Save or print the modified SQL
with open("modified_sql_file.sql", "w", encoding="utf-8") as file:
    file.write(updated_sql_text)

print("SQL file successfully modified!")
