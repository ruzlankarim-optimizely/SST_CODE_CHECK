import re
import os


def remove_sql_comments_and_quotes(sql_file_path, output_directory="cleaned_sql"):
    """
    Removes single-line and multi-line comments from an SQL file, preserving quoted strings.

    Args:
        sql_file_path: Path to the input SQL file.
        output_directory: Directory to save the cleaned SQL file. Defaults to "cleaned_sql".
    """
    try:
        with open(sql_file_path, "r") as file:
            sql_content = file.read()

        def replace_comments(match):
            """Helper function to replace comments while preserving quotes."""
            s = match.group(0)
            if s.startswith("--"):
                return ""
            elif s.startswith("/*"):
                return ""
            else:
                return s

        # Regular expression to match comments and quoted strings
        pattern = r"(--[^\r\n]*|/\*.*?\*/|'([^']|'')*')"
        sql_content = re.sub(pattern, replace_comments, sql_content, flags=re.DOTALL)

        # Remove trailing whitespace and newlines
        sql_content = sql_content.strip()

        # Create the output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)

        # Create output file name based on input file name
        input_file_name = os.path.basename(sql_file_path)
        output_file_name = f"cleaned_{input_file_name}"
        output_file_path = os.path.join(output_directory, output_file_name)

        with open(output_file_path, "w") as output_file:
            output_file.write(sql_content)

        print(f"Cleaned SQL saved to: {output_file_path}")

    except FileNotFoundError:
        print(f"Error: File not found: {sql_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Example usage:
remove_sql_comments_and_quotes("churn_migration_classifier.sql")
