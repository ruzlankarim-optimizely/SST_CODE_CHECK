import re


def transform_sql_file(input_file_path, output_file_path):
    """
    Transforms a SQL file by splitting a CASE statement in initial_table_3 CTE
    into separate columns and merging it back into the file.

    Args:
        input_file_path (str): Path to the input SQL file
        output_file_path (str): Path to write the transformed SQL file
    """
    # Read the SQL file
    with open(input_file_path, "r") as f:
        sql_content = f.read()

    # Find the initial_table_3 CTE
    initial_table_3_pattern = (
        r"initial_table_3\s+as\s*\(\s*select.*?from\s+initial_table_2\s*\)"
    )
    initial_table_3_match = re.search(initial_table_3_pattern, sql_content, re.DOTALL)

    if not initial_table_3_match:
        print("Could not find initial_table_3 CTE in the SQL file.")
        return

    initial_table_3_content = initial_table_3_match.group(0)

    # Find the CASE statement
    case_pattern = r"case\s*(.*?)end\s+as\s+\"Movement Classification\""
    case_match = re.search(case_pattern, initial_table_3_content, re.DOTALL)

    if not case_match:
        print("Could not find the CASE statement in initial_table_3 CTE.")
        return

    case_content = case_match.group(1)

    # Extract WHEN clauses
    when_pattern = r"when\s+(.*?)\s+then\s+\'(.*?)\'"
    when_matches = re.findall(when_pattern, case_content, re.DOTALL)

    # Create new columns for each WHEN clause
    new_columns = []
    for condition, classification in when_matches:
        # Transform classification into column name
        column_name = re.sub(r"[^a-zA-Z0-9]", "_", classification.lower()).strip("_")
        # Remove consecutive underscores
        column_name = re.sub(r"_+", "_", column_name)

        # Create a new column with the condition
        new_column = f"""
        CASE WHEN {condition.strip()} THEN 1 ELSE 0 END as {column_name}"""
        new_columns.append(new_column)

    # Reconstruct the initial_table_3 CTE with the new columns
    original_select_part = re.search(
        r"select\s*(.*?)case", initial_table_3_content, re.DOTALL
    ).group(1)

    new_cte = f"""
  initial_table_3 as (
    select {original_select_part.strip()}{','.join(new_columns)},
      case
{case_content}end as "Movement Classification"
    from initial_table_2 )"""

    # Replace the original CTE with the new one
    transformed_sql = sql_content.replace(initial_table_3_match.group(0), new_cte)

    # Write the transformed SQL to the output file
    with open(output_file_path, "w") as f:
        f.write(transformed_sql)

    print(f"Transformed SQL file written to {output_file_path}")
    print(f"Added {len(new_columns)} new columns based on CASE statement conditions")


# Example usage
if __name__ == "__main__":
    input_file = "cleaned_churn_migration_classifier.sql"
    output_file = "transformed.sql"
    transform_sql_file(input_file, output_file)
