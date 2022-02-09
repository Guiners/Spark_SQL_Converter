
from functions import SQLConverter
import click
import logging


@click.command()
@click.option("--schema_file_path", help="Path to SCHEMA.")
@click.option("--data_file_path", help="path to file where is DATA to query.")
@click.option(
    "--query_file_path",
    default="",
    help="Path where file will be saved. INCLUDE name of file with extension.",
)
def main(schema_file_path, data_file_path, query_file_path):
    converter = SQLConverter(schema_file_path, data_file_path, query_file_path)
    given_df = converter.load_json(data_file_path)
    allowed_df = converter.load_csv(converter.schema_path)
    converter.load_database_name(allowed_df)
    given_columns = converter.get_given_columns(given_df)
    allowed_columns = converter.get_allowed_columns(allowed_df)
    common_columns, uncommon_columns = converter.get_common_columns(
        allowed_columns, given_columns
    )
    given_df = converter.remove_uncommon_columns(given_df, uncommon_columns)
    common_columns = converter.compare_columns_position(common_columns, allowed_columns)
    given_df = converter.correct_columns_position(given_df, common_columns)
    allowed_data_types = converter.check_allowed_data_types(allowed_columns, allowed_df)
    sql_data = converter.create_sql_content(given_df)
    given_data_types = converter.check_given_data_types(sql_data, allowed_data_types)
    empty_sql = converter.create_empty_sql(allowed_columns)
    sql_data = converter.compare_data_types(
        allowed_data_types,
        given_data_types,
        allowed_columns,
        common_columns,
        sql_data,
        empty_sql,
    )
    variables = converter.choose_output_method(sql_data)
    logging.info("\nMission Complete!!!")


logging.getLogger().setLevel(logging.INFO)
if __name__ == "__main__":
    main()

