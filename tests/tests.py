from unittest import mock
import sys
import pytest
import random
import pandas as pd
import numpy as np
import os.path

sys.path.append("/Users/robertkozak/repos/spark-sql-converter/source_code")
from functions import SQLConverter

not_empty_string_var = "1234"
integer_var = 1234
float_var = 12.34
none_var = None
list_var = []
dict_var = {}
tuple_var = ()
empty_string_var = ""
false_var = False
true_var = True

python_data_types = [
    "string",
    "boolean",
    "None",
    "double",
    "array",
    "struct",
    "array(struct",
]
sql_data_types = ["ARRAY", "NAMED", "null", "ARRAY(NAMED", "whatever"]


class TestConverter:
    def create_converter(
        self,
        schema_file_path=mock.ANY,
        data_file_path=mock.ANY,
        query_file_path="",
    ):
        conv = SQLConverter(schema_file_path, data_file_path, query_file_path)
        conv.database = "Test"
        return conv

    class RunConverter:
        def __init__(self, converter):
            self.given_df_test = converter.load_json(converter.data_path)
            self.allowed_df_test = converter.load_csv(converter.schema_path)
            self.given_columns_test = converter.get_given_columns(self.given_df_test)
            converter.load_database_name(self.allowed_df_test)
            self.allowed_columns_test = converter.get_allowed_columns(self.allowed_df_test)
            (self.common_columns_test, self.uncommon_columns_test) = converter.get_common_columns(
                self.allowed_columns_test, self.given_columns_test
            )
            self.given_df_test = converter.remove_uncommon_columns(
                self.given_df_test, self.uncommon_columns_test
            )
            self.common_columns_test = converter.compare_columns_position(
                self.common_columns_test, self.allowed_columns_test
            )
            self.given_df_test = converter.correct_columns_position(
                self.given_df_test, self.common_columns_test
            )
            self.allowed_data_types_test = converter.check_allowed_data_types(
                self.allowed_columns_test, self.allowed_df_test
            )
            self.empty_sql_test = converter.create_empty_sql(self.allowed_columns_test)
            self.sql_data_test = converter.create_sql_content(self.given_df_test)
            self.given_data_types_test = converter.check_given_data_types(self.sql_data_test, self.allowed_data_types_test)
            self.sql_data_test = converter.compare_data_types(
                self.allowed_data_types_test,
                self.given_data_types_test,
                self.allowed_columns_test,
                self.common_columns_test,
                self.sql_data_test,
                self.empty_sql_test,
            )
            self.variables_test = converter.choose_output_method(self.sql_data_test)

    def test_convert_value_to_null(self):
        conv = self.create_converter()
        false_list = (integer_var, float_var)
        true_list = (
            none_var,
            not_empty_string_var,
            empty_string_var,
            list_var,
            dict_var,
            tuple_var,
        )
        for var_types in false_list:
            assert conv.convert_value_to_null(var_types) is None
        for var_types in true_list:
            assert conv.convert_value_to_null(var_types) == "null"
            assert type(conv.convert_value_to_null(var_types)) is str


    def test_is_empty(self):
        conv = self.create_converter()
        true_list = (
            list_var,
            dict_var,
            tuple_var,
            empty_string_var,
            none_var,
            false_var,
        )
        false_list = (not_empty_string_var, integer_var, float_var, true_var)
        for var_types in true_list:
            assert conv.is_empty(var_types)
        for var_types in false_list:
            assert conv.is_empty(var_types) is False

    def test_is_int(self):
        conv = self.create_converter()
        example1 = "12131331"
        example2 = "f33f333f"
        example3 = "44g32dwg333"
        example4 = "304982030939392"
        assert conv.is_int(example1)
        assert conv.is_int(example4)
        assert not conv.is_int(example2)
        assert not conv.is_int(example3)

    def test_bool_converter(self):
        conv = self.create_converter()
        assert conv.bool_converter("True")
        assert conv.bool_converter(True)
        assert conv.bool_converter("true")
        assert not conv.bool_converter("False")
        assert not conv.bool_converter(False)
        assert not conv.bool_converter("false")
        assert conv.bool_converter("example") is None

    def test_get_given_columns(self):
        conv = self.create_converter()
        columns_test = [
            "string",
            "boolean",
            "None",
            "double",
            "tuple",
            "name123",
            "pandas",
            "one",
        ]
        given_df_test = {}
        for i in range(len(columns_test)):
            given_df_test[columns_test[i]] = [random.randint(1, 100) for _ in range(15)]
        given_df_test = pd.DataFrame(given_df_test)
        given_columns_test = conv.get_given_columns(given_df_test)
        for column in given_columns_test:
            assert column in columns_test
        assert len(given_columns_test) == len(columns_test)

    def test_get_allowed_columns(self):
        conv = self.create_converter()
        columns_test = [
            "string",
            "boolean",
            "None",
            "double",
            "tuple",
            "name123",
            "pandas",
            "one",
        ]
        allowed_df_test = {
            "col_name": columns_test,
            "data_type": [x for x in range(len(columns_test))],
            "comment": [x for x in range(len(columns_test))],
        }

        allowed_df_test = pd.DataFrame(allowed_df_test)
        allowed_columns_test = conv.get_allowed_columns(allowed_df_test)
        assert len(allowed_columns_test) == len(columns_test)
        for column in allowed_columns_test:
            assert column in columns_test

    def test_get_common_columns(self):
        conv = self.create_converter()
        given_columns_test = [
            "string",
            "boolean",
            "None",
            "tuple",
            "pandas",
            "one",
        ]
        allowed_columns_test = ["boolean", "None", "double", "tuple", "one"]
        common_test = ["boolean", "None", "tuple", "one"]
        (common_columns_test, uncommon_columns_test) = conv.get_common_columns(
            allowed_columns_test, given_columns_test
        )
        assert len(common_columns_test) == len(common_test)
        assert common_columns_test == common_test
        for column in common_columns_test:
            assert column in given_columns_test
            assert column in allowed_columns_test
        for column in uncommon_columns_test:
            assert column in given_columns_test
            assert column not in common_columns_test

    def test_get_common_columns_empty(self):
        conv = self.create_converter()
        given_columns_test = ["string", "pandas", "name123", "321", "xyz"]
        allowed_columns_test = ["boolean", "None", "tuple", "one"]
        with pytest.raises(ValueError):
            (common_columns_test, uncommon_columns_test) = conv.get_common_columns(
                allowed_columns_test, given_columns_test
            )

    def test_compare_columns_position(self):
        conv = self.create_converter()
        given_columns_test = list(set("Lorem Ipsum is simply dummy text of the"))
        allowed_columns_test = list(
            set(" text of the printing and typesetting industry")
        )
        (common_columns_test, uncommon_columns_test) = conv.get_common_columns(
            allowed_columns_test, given_columns_test
        )
        compared_columns_test = conv.compare_columns_position(
            common_columns_test, allowed_columns_test
        )
        for column in common_columns_test:
            assert column in allowed_columns_test
            assert column in given_columns_test
        for index in range(len(compared_columns_test)):
            assert compared_columns_test[index][0] in allowed_columns_test
            assert compared_columns_test[index][0] in given_columns_test
            assert (
                allowed_columns_test[compared_columns_test[index][1]]
                == compared_columns_test[index][0]
            )

    def test_remove_uncommon_columns(self):
        conv = self.create_converter()
        given_df_test = pd.DataFrame(
            np.array(
                [
                    [i for i in range(10)],
                    [i for i in range(10, 20)],
                    [i for i in range(20, 30)],
                    [i for i in range(30, 40)],
                ]
            )
        )

        given_df_test.columns = [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "h",
            "i",
            "j",
        ]
        uncommon_columns_test = ["a", "c", "g", "f", "i"]
        common_columns_test = ["b", "d", "e", "h", "j"]
        given_df_removed_test = conv.remove_uncommon_columns(
            given_df_test, uncommon_columns_test
        )
        assert len(given_df_removed_test.columns.tolist()) == len(common_columns_test)
        assert given_df_removed_test.columns.tolist() == common_columns_test
        assert uncommon_columns_test not in given_df_removed_test.columns.tolist()
        for item in given_df_removed_test.columns.tolist():
            assert item in given_df_test.columns

    def test_correct_columns_positions(self):
        conv = self.create_converter()
        given_df_test = pd.DataFrame(
            np.array(
                [
                    [i for i in range(10)],
                    [i for i in range(10, 20)],
                    [i for i in range(20, 30)],
                    [i for i in range(30, 40)],
                ]
            )
        )

        given_df_test.columns = [
            "q",
            "w",
            "e",
            "r",
            "t",
            "y",
            "u",
            "i",
            "o",
            "p",
        ]
        common_columns_test = [
            "y",
            "u",
            "i",
            "o",
            "p",
            "q",
            "w",
            "e",
            "r",
            "t",
        ]
        corrected_columns_pos = conv.correct_columns_position(
            given_df_test, common_columns_test
        )
        assert len(corrected_columns_pos.columns.tolist()) == len(common_columns_test)
        assert corrected_columns_pos.columns.tolist() == common_columns_test
        assert not given_df_test.loc[1].equals(corrected_columns_pos.iloc[1])

    def test_check_allowed_data_types_random(self):
        conv = self.create_converter()
        common_columns_test = [x for x in range(48)]

        # we use this variable to count rows, content of it not important

        data_types = [random.choice(python_data_types) for _ in range(48)]
        data = {"a": common_columns_test, "b": data_types, "c": common_columns_test}
        allowed_df_test = pd.DataFrame(data)
        allowed_data_types_test = conv.check_allowed_data_types(
            common_columns_test, allowed_df_test
        )
        assert len(allowed_df_test) == len(allowed_data_types_test)
        for item in allowed_data_types_test:
            assert item in [
                "string",
                "boolean",
                "null",
                "float",
                ("list", "dict"),
                "list",
                "dict",
            ]

    def test_check_allowed_data_types(self):
        conv = self.create_converter()
        common_columns_test = [x for x in range(14)]
        data_types = [
            "string",
            "boolean",
            "None",
            "double",
            "array(struct",
            "string",
            "boolean",
            "None",
            "double",
            "array",
            "struct",
            "array(struct",
            "string",
            "boolean",
        ]
        data = {"a": common_columns_test, "b": data_types, "c": common_columns_test}
        allowed_df_test = pd.DataFrame(data)
        allowed_data_types_test = conv.check_allowed_data_types(
            common_columns_test, allowed_df_test
        )
        assert allowed_data_types_test == [
            "string",
            "boolean",
            "null",
            "float",
            ("list", "dict"),
            "string",
            "boolean",
            "null",
            "float",
            "list",
            "dict",
            ("list", "dict"),
            "string",
            "boolean",
        ]
        assert len(allowed_data_types_test) == len(data_types)

    def test_check_given_data_types_random(self):
        conv = self.create_converter()
        given_df_test = []
        container = []
        allowed_data_types_test = []
        data_types_counter = 0
        for _ in range(100):
            for _ in range(38):
                if random.random() > 0.6:
                    container.append(random.choice(sql_data_types))
                    data_types_counter += 1
            given_df_test.append(container)
            container = []
        given_data_types_test = conv.check_given_data_types(given_df_test, allowed_data_types_test)
        assert len(given_data_types_test) == data_types_counter
        for item in given_data_types_test:
            assert item in [
                "string",
                "boolean",
                "null",
                "double",
                ("list", "dict"),
                "list",
                "dict",
            ]

    def test_check_given_data_types(self):
        conv = self.create_converter()
        allowed_data_types_test = []
        given_df_test = [
            ["ARRAY"],
            ["NAMED"],
            ["ARRAY(NAMED"],
            ["whatever"],
            ["NAMED"],
            ["null"],
            ["ARRAY(NAMED"],
            ["whatever"],
            ["ARRAY"],
            ["whatever"],
        ]
        given_data_types_test = conv.check_given_data_types(given_df_test, allowed_data_types_test)
        assert len(given_data_types_test) == len(given_df_test)
        for item in given_data_types_test:
            assert item in [
                "string",
                "boolean",
                "null",
                "double",
                ("list", "dict"),
                "list",
                "dict",
                "int",
                "float",
            ]

    def test_reformat_data(self):
        conv = self.create_converter()
        example1 = [
            {
                "id": None,
                "extension": None,
                "use": "home",
                "_use": None,
                "type": None,
                "_type": None,
                "text": None,
                "_text": None,
                "line": ["1234 Main Street"],
                "_line": None,
                "city": "Tampa",
                "_city": None,
                "district": None,
                "_district": None,
                "state": "FL",
                "_state": None,
                "postalCode": "33000",
                "_postalCode": None,
                "country": "US",
                "_country": None,
                "period": None,
            }
        ]

        test1 = conv.reformat_data(example1)
        for sign in test1:
            assert sign not in ["{", "[", "}", "]"]
        assert "null" in test1
        assert "NAMED_STRUCT(" in test1
        assert "ARRAY(" in test1

    def test_reformat_data2(self):
        conv = self.create_converter()
        data_test = [
            "None",
            "None",
            "None",
            "None",
            "{",
            "{",
            "{",
            "{",
            "}",
            "}",
            "}",
            " }",
            "[",
            "[",
            "[",
            "[",
            "]",
            "]",
            "]",
            "]",
            "]",
        ]
        reformatted_data_test = conv.reformat_data(data_test)
        for sign in reformatted_data_test:
            assert sign not in data_test
        assert "null" in reformatted_data_test
        assert "NAMED_STRUCT(" in reformatted_data_test
        assert "ARRAY(" in reformatted_data_test

    def test_remove_unnecessary_quotes(self):
        conv = self.create_converter()
        sql_data_types2 = [
            '"ARRAY',
            '"NAMED',
            '"null"',
            ')"',
            "[",
            "]",
        ]
        variables = [random.choice(sql_data_types2) for _ in range(100)]
        filtrated_data_test = conv.remove_unnecessary_quotes(variables)
        for item in filtrated_data_test:
            assert item not in [
                "[",
                "]",
                '"A',
                '"N',
                ')"',
                "'A",
                "'N",
            ]

    def test_create_sql_content(self):
        conv = self.create_converter()
        example1 = [
            {
                "id": None,
                "extension": None,
                "use": "home",
                "_use": None,
                "type": None,
                "_type": None,
                "text": None,
                "_text": None,
                "line": ["1234 Main Street"],
                "_line": None,
                "city": "Tampa",
                "_city": None,
                "district": None,
                "_district": None,
                "state": "FL",
                "_state": None,
                "postalCode": "33000",
                "_postalCode": None,
                "country": "US",
                "_country": None,
                "period": None,
            }
        ]

        data_test = pd.DataFrame(example1)
        sql_content_test = conv.create_sql_content(data_test)
        for var in sql_content_test:
            for item in var:
                assert item not in [
                    "{",
                    "[",
                    "}",
                    "]",
                    "None",
                ]

    def test_create_sql_content_random(self):
        conv = self.create_converter()
        python_data_types2 = [
            "None",
            "{",
            "[",
            "}",
            "]",
        ]
        columns_test = [x for x in range(38)]

        rows_test = [
            [
                (random.choice(python_data_types2) if random.random() > 0.4 else None)
                for _ in range(3)
            ]
            for _ in range(38)
        ]
        zipped = zip(columns_test, rows_test)
        dictionary = dict(zipped)
        df_test = pd.DataFrame(dictionary)
        sql_content_test = conv.create_sql_content(df_test)
        for var in sql_content_test:
            for item in var:
                assert item not in [
                    "{",
                    "[",
                    "}",
                    "]",
                    ":",
                    "None",
                ]
                assert item in [
                    "null",
                    "NAMED_STRUCT(",
                    "ARRAY(",
                    ")",
                    "(",
                    ",",
                ]

    def test_create_empty_sql(self):
        conv = self.create_converter()
        allowed_columns_test = [number for number in range(100)]
        empty_sql_test = conv.create_empty_sql(allowed_columns_test)
        for item in empty_sql_test:
            assert item == "null"
        assert len(empty_sql_test) == len(allowed_columns_test)

    def test_compare_data_types1(self):
        conv = self.create_converter()
        allowed_data_types = [
            "string",
            "string",
            "dict",
            "string",
            "dict",
            "string",
            "dict",
            "dict",
            ("list", "dict"),
            ("list", "dict"),
        ]
        given_data_types = [
            "string",
            "string",
            "dict",
            "string",
            "dict",
            "string",
            "dict",
            "dict",
            ("list", "dict"),
            ("list", "dict"),
            "string",
            "string",
            "dict",
            "string",
            "dict",
            "string",
            "dict",
            "dict",
            ("list", "dict"),
            ("list", "dict"),
        ]
        allowed_columns_test = [
            "resourceType",
            "id",
            "meta",
            "implicitRules",
            "_implicitRules",
            "language",
            "_language",
            "text",
            "contained",
            "extension",
        ]
        common_columns_test = [
            ["resourceType", 0],
            ["id", 1],
            ["meta", 2],
            ["implicitRules", 3],
            ["_implicitRules", 4],
            ["language", 5],
            ["_language", 6],
            ["text", 7],
            ["contained", 8],
            ["extension", 9],
        ]
        sql_data_test = [
            [
                "Patient",
                "null",
                "NAMED_STRUCT('id', null, 'extension', null, 'versionId', null, '_versionId', null, 'lastUpdated', '2021-09-21T23,21,06.932000 00,00', '_lastUpdated', null, 'source', null,'_source', null, 'profile', null, 'security', null, 'tag', null)",
                "null",
                "null",
                "null",
                "null",
                "null",
                "null",
                "null",
            ],
            [
                "Patient",
                "null",
                "NAMED_STRUCT('id', null, 'extension', null, 'versionId', null, '_versionId', null, 'lastUpdated', '1999-12-12T33,17,09.932000 00,00', '_lastUpdated', null, 'source', null, '_source', null, 'profile', null,'security', null, 'tag', null)",
                "null",
                "null",
                "null",
                "null",
                "null",
                "null",
                "null",
            ],
        ]
        empty_sql_test = ["null" for _ in range(len(allowed_columns_test))]
        compared_sql_data_test = conv.compare_data_types(
            allowed_data_types,
            given_data_types,
            allowed_columns_test,
            common_columns_test,
            sql_data_test,
            empty_sql_test,
        )
        assert len(compared_sql_data_test) == len(sql_data_test)
        for item in range(len(sql_data_test)):
            assert len(compared_sql_data_test[item]) == len(sql_data_test[item])
        assert "NAMED_STRUCT(" in compared_sql_data_test[0][2]
        assert compared_sql_data_test[0][3] == "null"
        assert compared_sql_data_test[0][-1] == "null"
        assert compared_sql_data_test[1][4] == "null"

    def test_compare_data_types_error(self):
        conv = self.create_converter()
        allowed_data_types = [
            "string",
            "string",
            "dict",
            "string",
            "dict",
            "string",
            "dict",
            "dict",
            ("list", "dict"),
            ("list", "dict"),
        ]
        given_data_types = [
            "string",
            "null",
            "dict",
            "null",
            "null",
            "null",
            "null",
            "null",
            "null",
            "null",
            "string",
            "null",
            "dict",
            "null",
            "null",
            "null",
            "null",
            "null",
            "null",
            "null",
        ]
        allowed_columns_test = [
            "resourceType",
            "id",
            "meta",
            "implicitRules",
            "_implicitRules",
            "language",
            "_language",
            "text",
            "contained",
            "extension",
        ]
        common_columns_test = [
            ["error", 0],
            ["id", 1],
            ["meta", 2],
            ["implicitRules", 3],
            ["_implicitRules", 4],
            ["language", 5],
            ["_language", 6],
            ["text", 7],
            ["contained", 8],
            ["extension", 9],
        ]
        sql_data_test = [
            [
                "Patient",
                "null",
                "NAMED_STRUCT('id', null, 'extension', null, 'versionId', null, '_versionId', null, 'lastUpdated', '2021-09-21T23,21,06.932000 00,00', '_lastUpdated', null, 'source', null, '_source', null, 'profile', null, 'security', null, 'tag', null)",
                "null",
                "null",
                "null",
                "null",
                "null",
                "null",
                "null",
            ]
        ]

        with pytest.raises(Exception):
            compared_sql_data_test = conv.compare_data_types(
                allowed_data_types,
                given_data_types,
                allowed_columns_test,
                common_columns_test,
                sql_data_test,
            )

    def test_choose_output_method_and_output_sql(self):
        conv_print_test = self.create_converter()
        conv_file_test = self.create_converter(
            query_file_path="tests_data/test_query.txt"
        )
        sql_data_test = [
            [
                (random.choice(python_data_types) if random.random() > 0.4 else None)
                for _ in range(38)
            ]
            for _ in range(88)
        ]
        print_values_test = conv_print_test.choose_output_method(sql_data_test)
        file_values_test = conv_file_test.choose_output_method(sql_data_test)
        assert os.path.isfile("tests_data/test_query.txt")
        assert len(print_values_test) == len(file_values_test) and len(
            file_values_test
        ) == len(sql_data_test)
        assert len(print_values_test) == len(sql_data_test) and len(
            file_values_test
        ) == len(sql_data_test)
        assert file_values_test[-1][-1] == ";"
        assert print_values_test[-1][-1] == ";"

    def test_final1(self):
        converter = self.create_converter(
            "tests_data/test_case1/Schema_file_test1.csv",
            "tests_data/test_case1/test_data_file1.json",
            "tests_data/test_case1/test_output1.txt",
        )
        test = self.RunConverter(converter)
        myfile = open("tests_data/test_case1/test_output1.txt", "r")
        output_test = myfile.read()
        query_test = """INSERT INTO databasenamelalalalalala VALUES
('Test1', '12345', NAMED_STRUCT('id':'lll','extension':null,'versionId':null,'_versionId':null,'lastUpdated':'000002220202020202','_lastUpdated':null,'source':null,'_source':null,'profile':null,'security':null,'tag':null), null, null, null, null, null, null, null, null);"""
        assert output_test == query_test
        assert converter.database_name == "databasenamelalalalalala"
        assert converter.query == "INSERT INTO databasenamelalalalalala VALUES"
        assert os.path.isfile(converter.data_path)
        assert test.sql_data_test[0][1] == "12345"
        assert test.sql_data_test[0][3] == "null"
        assert test.sql_data_test[0][6] == "null"
        assert test.sql_data_test[0][0] == "Test1"

    def test_final_array_of_structs(self):
        converter = self.create_converter(
            "tests_data/test_case2/Schema_file_test2.csv",
            "tests_data/test_case2/test_data_file2.json",
            "tests_data/test_case2/test_output2.txt",
        )
        test = self.RunConverter(converter)
        myfile = open("tests_data/test_case2/test_output2.txt", "r")
        output_test = myfile.read()
        assert converter.database_name == "databasenamelalalalalala"
        assert converter.query == "INSERT INTO databasenamelalalalalala VALUES"
        assert os.path.isfile(converter.data_path)
        assert test.sql_data_test[0][12] == "True"
        assert test.sql_data_test[0][16] == "male"
        assert test.sql_data_test[0][15] == "null"
        assert test.sql_data_test[0][11] == "ARRAY(NAMED_STRUCT('id':null,'extension':null,'use':null,'_use':null,'type':null,'system':'https://github.com/synthetichealth/synthea','_system':null,'value':'3','_value':null,'period':null,'assigner':null),NAMED_STRUCT('id':null,'extension':null,'use':null,'_use':null,'type':null,'system':'https://jnj.com/dsep','_system':null,'value':'9ae56591-6620-474c-87bc-002888501e84','_value':null,'period':null,'assigner':null))"
        assert test.sql_data_test[0][14] == "ARRAY(NAMED_STRUCT('id':null,'extension':null,'use':'official','_use':null,'text':null,'_text':null,'family':'Chalmers','_family':null,'given':ARRAY('Peter','James'),'_given':null,'prefix':null,'_prefix':null,'suffix':null,'_suffix':null,'period':null))"

    def test_load_json_empty(self):
        conv = self.create_converter()
        conv.data_path = "tests_data/test_case3/test_data_file3.json"
        with pytest.raises(ValueError):
            given_df_test = conv.load_json(conv.data_path)

    def test_load_csv_empty(self):
        conv = self.create_converter()
        conv.schema_path = "tests_data/test_case3/Schema_file_test3.csv"
        with pytest.raises(ValueError):
            allowed_df_test = conv.load_csv(conv.schema_path)

    def test_load_json_wrong_extension(self):
        conv = self.create_converter()
        conv.data_path = "tests_data/test_case3/Schema_file_test3.csv"
        with pytest.raises(ValueError):
            given_df_test = conv.load_json(conv.data_path)

    def test_load_csv_wrong_extension(self):
        conv = self.create_converter()
        conv.schema_path = "tests_data/test_case3/test_data_file3.json"
        with pytest.raises(ValueError):
            allowed_df_test = conv.load_csv(conv.schema_path)

    def test_database_name_empty(self):
        conv = self.create_converter()
        conv.schema_path = "tests_data/test_case4/Schema_file_test4.csv"
        allowed_df_test = conv.load_csv(conv.schema_path)
        with pytest.raises(ValueError):
            conv.load_database_name(allowed_df_test)

    def test_load_json_string_instead_of_columns(self):
        conv = self.create_converter()
        conv.data_path = "tests_data/test_case5/test_data_file5.json"
        with pytest.raises(ValueError):
            given_df_test = conv.load_json(conv.data_path)

    def test_one_column_data(self):
        converter = self.create_converter(
                "tests_data/test_case6/Schema_file_test6.csv",
                "tests_data/test_case6/data_file_test6.json",
                "tests_data/test_case6/test_output6.txt",
            )
        test = self.RunConverter(converter)
        myfile = open("tests_data/test_case6/test_output6.txt", "r")
        output_test = myfile.read()
        query_test = """INSERT INTO test_case6 VALUES
(null, null, null, 'test123456', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""
        assert converter.database_name == "test_case6"
        assert converter.query == "INSERT INTO test_case6 VALUES"
        assert os.path.isfile(converter.data_path)
        assert output_test == query_test
        assert test.sql_data_test[0][3] == "test123456"
        assert test.sql_data_test[0][12] == "null"
        assert test.sql_data_test[0][1] == "null"

    def test_load_csv_columns_without_index2(self):
        conv = self.create_converter()
        conv.schema_path = "tests_data/test_case5/Schema_file_test5.csv"
        allowed_df_test = conv.load_csv(conv.schema_path)
        allowed_columns_test = conv.get_allowed_columns(allowed_df_test)


