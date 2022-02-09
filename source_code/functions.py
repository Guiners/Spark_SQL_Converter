
import pandas as pd
import numpy as np
import os
from typing import Tuple, Any, Union, List
import logging
import click


class SQLConverter:
    def __init__(self, schema_path: str, data_path: str, query_file_path=""):
        self.schema_path = schema_path
        self.data_path = data_path
        self.database_name = ""
        self.query_file_path = query_file_path
        self.query = ""

    def load_csv(self, data_path: str) -> pd.DataFrame:
        try:
            csv = pd.read_csv(str(data_path))
            return csv
        except IndexError:
            raise ValueError(
                "Schema file is badly formatted"
            )
        except ValueError:
            raise ValueError(
                "Input Schema file is empty"
            )
        except FileNotFoundError:
            raise ValueError(
                "Wrong file extension, CSVs expected"
            )

    def load_json(self, data_path: str) -> pd.DataFrame:
        try:
            return pd.read_json(str(data_path))
        except ValueError:
            raise ValueError(
                "Input data_file is empty, badly formatted or wrong file extension, JSON expected"
            )

    def load_database_name(self, allowed_df: pd.DataFrame):
        self.database_name = allowed_df.loc[allowed_df["col_name"] == "Name"].values[0][
            1
        ]
        if self.is_empty(self.database_name) or str(self.database_name) == 'nan':
            raise ValueError(
                "Database name is empty!!! Check 'name' column in Schema File"
            )

        else:
            self.query = "INSERT INTO {} VALUES".format(str(self.database_name))

    def is_empty(self, value: Any) -> bool:
        if type(value) is np.bool_:
            return False
        elif value == "":
            return True
        elif value == " ":
            return True
        else:
            if not value:
                return True
            else:
                return False

    def is_int(self, variable: Any):
        try:
            for value in range(len(variable)):
                int(variable[value])
        except ValueError:
            return False
        except TypeError:
            return False
        else:
            return True

    def check_int_in_columns(self, allowed_data_types: List[str]) -> bool:
        for column in allowed_data_types:
            if column == "int":
                return True
        return False

    def bool_converter(self, value: Any) -> bool:
        if value == "True":
            return True
        elif value == "False":
            return False
        elif value == "true":
            return True
        elif value == "false":
            return False
        if value is False:
            return False
        elif value is True:
            return True

    def convert_value_to_null(self, value: Any) -> Union[str, bool]: #change name or usage
        if value is None:
            return "null"
        elif type(value) is str:
            return "null"
        elif self.is_empty(value):
            return "null"

    def get_given_columns(self, given_df: pd.DataFrame) -> List[str]:
        return list(given_df.columns)

    def get_allowed_columns(self, allowed_df: pd.DataFrame) -> List[str]:
        all_columns = allowed_df[list(allowed_df.columns)[0]].tolist()
        allowed_columns = []
        for column in all_columns:
            if str(column) == 'nan':
                break
            else:
                allowed_columns.append(column)
        return allowed_columns

    def get_common_columns(
        self, allowed_columns: List[str], given_columns: List[str]
    ) -> Tuple[list, list]:
        common_columns = []
        uncommon_columns = []
        [
            common_columns.append(name)
            if name in allowed_columns
            else uncommon_columns.append(name)
            for name in given_columns
        ]
        if len(common_columns) == 0:
            raise ValueError(
                    "There is no common_columns name between SCHEMA file and GIVEN file."
                    "Check if you uploaded right file."
            )
        return common_columns, uncommon_columns

    def remove_uncommon_columns(
        self, given_df: pd.DataFrame, uncommon_columns: List[str]
    ) -> pd.DataFrame:
        given_df1 = given_df.copy()
        if len(uncommon_columns) > 0:
            logging.warning(uncommon_columns)
            logging.warning(
                "These columns were not found in schema file. They won't be included in query.\n"
            )
            for i in uncommon_columns:
                del given_df1[i]
        return given_df1

    def compare_columns_position(
        self, common_columns: List[str], allowed_columns: List[str]
    ) -> List[str]:
        common_columns_compared = []
        counter = 0
        for allowed_col in allowed_columns:
            for given_col in common_columns:
                if given_col == allowed_col:
                    common_columns_compared.append([given_col, counter])
                    break
            counter += 1
        return common_columns_compared

    def correct_columns_position(
        self, given_df: pd.DataFrame, common_columns: List[str]
    ) -> pd.DataFrame:
        given_df1 = given_df.copy()
        return given_df1[[x[0] for x in common_columns]]

    def create_empty_sql(self, allowed_columns: List[str]) -> List[str]:
        empty_sql = []
        for _ in range(len(allowed_columns)):
            empty_sql.append("null")
        return empty_sql

    def check_allowed_data_types(
        self, common_columns: List[str], allowed_df: pd.DataFrame
    ) -> List[str]:
        allowed_data_types = []
        for column in range(len(common_columns)):
            if type(allowed_df.iloc[column][1]) is float:
                allowed_data_types.append("null")
            elif allowed_df.iloc[column][1][:3] == "int":
                allowed_data_types.append("int")
            elif allowed_df.iloc[column][1][:5] == "float":
                allowed_data_types.append("float")
            elif allowed_df.iloc[column][1][:6] == "integer":
                allowed_data_types.append("int")
            elif allowed_df.iloc[column][1][:6] == "struct":
                allowed_data_types.append("dict")
            elif allowed_df.iloc[column][1][:6] == "string":
                allowed_data_types.append("string")
            elif allowed_df.iloc[column][1][:7] == "boolean":
                allowed_data_types.append("boolean")
            elif allowed_df.iloc[column][1][:6] == "double":
                allowed_data_types.append("float")
            elif allowed_df.iloc[column][1][:5] == "array":
                if allowed_df.iloc[column][1][6:12] == "struct":
                    allowed_data_types.append(("list", "dict"))
                else:
                    allowed_data_types.append("list")
            elif type(allowed_df.iloc[column][1]) == str:
                if allowed_df.iloc[column][1] == "None":
                    allowed_data_types.append("null")
        return allowed_data_types

    def check_given_data_types(self, given_df_variables: List[str], allowed_data_types: List[str]) -> List[str]:
        given_data_types = []
        for lst in given_df_variables:
            for data in lst:
                if type(data) is bool:
                    given_data_types.append("boolean")
                    continue

                if self.is_int(data):
                    if self.check_int_in_columns(allowed_data_types):
                        given_data_types.append("int")
                        continue
                    else:
                        given_data_types.append("string")

                elif data[:5] == "ARRAY":
                    if data[6:11] == "NAMED":
                        given_data_types.append(("list", "dict"))
                    else:
                        given_data_types.append("list")
                elif data[:5] == "NAMED":
                    given_data_types.append("dict")
                elif data[:4] == "null":
                    given_data_types.append("null")
                elif data[:4] == "True":
                    given_data_types.append("boolean")
                elif data[:5] == "False":
                    given_data_types.append("boolean")
                else:
                    given_data_types.append("string")

        return given_data_types

    def reformat_data(self, data: List[str]) -> str:
        string_data = str(data)
        string_data = string_data.replace("None", "null")
        data = list(string_data)
        for sign in range(len(data)):
            if data[sign] == "{":
                data[sign] = "NAMED_STRUCT("
            elif data[sign] == "[":
                data[sign] = "ARRAY("
            elif data[sign] == "}" or data[sign] == "]":
                data[sign] = ")"
            elif data[sign] == " ":
                data[sign] = ""
            elif data[sign] == '"':
                data[sign] = "'"

        return "".join(data)

    def compare_data_types(
        self,
        allowed_data_types: list,
        given_data_types: list,
        allowed_columns: List[str],
        common_columns: List[str],
        sql_data,
        empty_sql,
    ) -> list:
        data_type_compared = []
        data_type_counter = 0
        for var in sql_data:
            var_counter = 0
            brackets = empty_sql.copy()
            for col_name in common_columns:
                if allowed_columns[col_name[1]] == col_name[0]:
                    if given_data_types[data_type_counter] == "null":
                        brackets[col_name[1]] = "null"
                    elif (
                        given_data_types[data_type_counter]
                        != allowed_data_types[var_counter]
                    ):
                        raise ValueError(

                                "In column {}, given data type: {}, allowed data type: {} ".format(
                                    allowed_columns[col_name[1]],
                                    given_data_types[data_type_counter],
                                    allowed_data_types[var_counter],

                            )
                        )
                    elif (
                        given_data_types[data_type_counter]
                        == allowed_data_types[var_counter]
                    ):
                        brackets[col_name[1]] = var[var_counter]
                    else:
                        if self.is_int(var[var_counter]):
                            if self.check_int_in_columns(allowed_data_types):
                                brackets[col_name[1]] = int(var[var_counter])
                            else:
                                brackets[col_name[1]] = str(var[var_counter])

                else:
                    raise Exception(logging.fatal("Columns position doesn't match"))
                var_counter += 1
                data_type_counter += 1

            data_type_compared.append(brackets)
        return data_type_compared

    def remove_unnecessary_quotes(self, data: List[str]) -> str:
        unnecessary_quotes = [
            ("[", ""),
            ("]", ""),
            ('"null"', "null"),
            ("'null'", "null"),
            ('"true"', "true"),
            ("'true'", "true"),
            ('"false"', "false"),
            ("'false'", "false"),
            ('"A', "A"),
            ('"N', "N"),
            (')"', ")"),
            ("'A", "A"),
            ("'N", "N"),
            ('"', "'"),
        ]
        data = str(data)
        for quote, correction in unnecessary_quotes:
            data = data.replace(quote, correction)
        return data

    def create_sql_content(self, given_df_filtrated) -> list:
        sql_content = []
        for row in range(len(given_df_filtrated)):
            sql_data = []
            for data in given_df_filtrated.iloc[row]:
                if self.is_empty(data):
                    if type(data) is bool:
                        sql_data.append(data)
                    else:
                        sql_data.append(self.convert_value_to_null(data))

                else:
                    sql_data.append(self.reformat_data(data))

            sql_content.append(sql_data)
        return sql_content

    def output_sql(self, sql_data: List[str], file="") -> list:
        values = []
        for data_index in range(len(sql_data)):
            data = sql_data[data_index]
            data = self.remove_unnecessary_quotes(data)
            if data_index == len(sql_data) - 1:
                if self.is_empty(file):
                    formatted_data = "({});".format(data)
                    print(formatted_data)
                else:
                    formatted_data = "\n({});".format(data)
                    file.write(formatted_data)
            else:
                if self.is_empty(file):
                    formatted_data = "({}),".format(data)
                    print(formatted_data)
                else:
                    formatted_data = "\n({}),".format(data)
                    file.write(formatted_data)
            values.append(formatted_data.replace("\n", ""))
        if self.is_empty(file):
            logging.info("\nYour SQL query is above :)")
        else:
            logging.info(
                "Txt file was saved to path: {path}".format(
                    path=os.path.join(self.query_file_path)
                )
            )
        return values

    def choose_output_method(self, sql_data: List[str]):
        if self.is_empty(self.query_file_path):
            print(self.query)
            return self.output_sql(sql_data)
        else:
            with open(os.path.join(self.query_file_path), "w") as txt_file:
                txt_file.write(self.query)
                return self.output_sql(sql_data, txt_file)

