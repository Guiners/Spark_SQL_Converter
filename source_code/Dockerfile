FROM python:3.8

ADD functions.py .
ADD spark-sql-converter.py .

RUN pip install pytest pandas numpy click

ENV schema_file_path ""
#ENV data_file_path ""
#ENV query_file_path ""

#CMD ["python", "./spark-sql-converter.py {}"]
ENTRYPOINT ['spark-sql-converter.py $schema_file_path']