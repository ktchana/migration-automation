import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import random
from datetime import *
import string
from decimal import Decimal


def generate_random_data(spark, table_names, num_records=10, ranges={}, foreign_keys={}, string_values={}, batch_size=10000):
    """Generates random data for multiple Hive tables and writes it to the tables,
    handling foreign keys relationships and allowing user to specify possible string values.

    Args:
        spark: SparkSession object.
        table_names: A list of Hive table names.
        num_records: Number of records to generate for each table.
        ranges: A dictionary of table names as keys, and dictionaries of column names 
               and their corresponding ranges as values.
               For integer/bigint/smallint/tinyint, specify a tuple (min, max).
               For double/decimal, specify a tuple (min, max).
               For timestamp, specify a tuple of datetime objects (start_date, end_date).
               For char/varchar, specify the maximum length (int).
               For date, specify a tuple of datetime objects (start_date, end_date).
        foreign_keys: A dictionary of foreign key relationships. Each key is a tuple
               representing a (source_table, source_column) pair, and the value is a 
               tuple representing the (target_table, target_column) pair.
               Example: foreign_keys = {
                           ("table1", "customer_id"): ("table2", "customer_id")
                       }
        string_values: A dictionary of table names as keys, and dictionaries of column names 
               and their corresponding lists of possible string values as values.
               Example: string_values = {
                           "table1": {
                               "status": ["active", "inactive"]
                           }
                       }
    """

    # Create a dictionary to store generated data for each table
    table_data = {}

    # Generate data for each table
    for table_name in table_names:
        # Read schema from Hive table
        schema = spark.sql(f"DESCRIBE {table_name}").collect()
        schema_dict = {row['col_name']: row['data_type'] for row in schema}

        # Create StructType from schema
        struct_fields = [StructField(name, get_spark_type(data_type), True) 
                         for name, data_type in schema_dict.items()]
        schema = StructType(struct_fields)

        if table_name not in table_data:
            table_data[table_name] = {}
        # Generate random data for the table
        data = []
        current_batch_size = 0
        for current_record_index in range(num_records):
            record = {}
            for name, data_type in schema_dict.items():
                if name not in table_data[table_name]:
                    table_data[table_name][name] = []

                table_ranges = ranges.get(table_name, {})
                table_string_values = string_values.get(table_name, {})

                # Handle foreign keys
                if (table_name, name) in foreign_keys:
                    parent_table, parent_column = foreign_keys[(table_name, name)]
                    # Reuse existing value from the parent table
                    if parent_table in table_data and parent_column in table_data[parent_table]:
                        record[name] = random.choice(table_data[parent_table][parent_column])
                    else:
                        # Generate a new value if not already generated
                        record[name] = generate_random_value(schema_dict[name], table_ranges.get(name), table_string_values.get(name))
                        # Store the generated value for later use
                        if parent_table not in table_data:
                            table_data[parent_table] = {}
                        if parent_column not in table_data[parent_table]:
                            table_data[parent_table][parent_column] = []
                        table_data[parent_table][parent_column].append(record[name])
                else:
                    # Generate a random value if not a foreign key
                    if table_ranges and name in table_ranges:
                        record[name] = generate_random_value(data_type, table_ranges[name], table_string_values.get(name))
                    elif table_string_values and name in table_string_values:
                        record[name] = random.choice(table_string_values[name])
                    else:
                        record[name] = generate_random_value(data_type)
                table_data[table_name][name].append(record[name])
            data.append(record)

            current_batch_size += 1
            if current_batch_size >= batch_size:
                print(f"Writing records {current_record_index - batch_size + 1} to {current_record_index} to {table_name} ...")
                df = spark.createDataFrame(data, schema)
                df.write.mode("append").format("hive").saveAsTable(table_name)
                data = []
            

        # Create DataFrame and write to Hive table
        print(f"Writing remaining records to {table_name} ...")
        df = spark.createDataFrame(data, schema)
        #df.show()
        df.write.mode("append").format("hive").saveAsTable(table_name)

def get_spark_type(data_type):
    """Maps Hive data types to Spark data types."""
    mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "smallint": ShortType(),
        "tinyint": ByteType(),
        "char": StringType(),
        "varchar": StringType(),
        "date": DateType(),
        "decimal": DecimalType(10, 2),
        "boolean": BooleanType()
        # Add more data type mappings as needed
    }
    data_type_string = data_type.lower().split('(')[0]
    if data_type_string == "decimal":
        d_scale, d_precision = map(int,  [s for s in "decimal(10,0)".strip("decimal()").split(',')])
        return DecimalType(d_scale, d_precision)
    else:
        return mapping.get(data_type_string, StringType())


def generate_random_value(data_type, range=None, string_values=None):
    """Generates a random value based on the data type, range (if specified),
    and possible string values (if specified).
    """
    if data_type.lower() == "string":
        if string_values:
            return random.choice(string_values)
        else:
            return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=10))
    elif data_type.lower() in ("int", "bigint", "smallint", "tinyint"):
        if range:
            return random.randint(range[0], range[1])
        else:
            if data_type.lower() == "smallint":
                return random.randint(0, 32767)
            elif data_type.lower() == "tinyint":
                return random.randint(0, 255)
            else:
                return random.randint(0, 1000000000)
    elif data_type.lower() == "double":
        if range:
            return random.uniform(range[0], range[1])
        else:
            return random.uniform(0, 1000)
    elif data_type.lower() == "timestamp":
        if range:
            start_date, end_date = range
            timestamp = random.randrange(int(start_date.timestamp()), int(end_date.timestamp()))
            return datetime.fromtimestamp(timestamp)
        else:
            return datetime.now()
    elif data_type.lower().startswith("char"):
        if range:
            return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=range))
        else:
            return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=10))
    elif data_type.lower().startswith("varchar"):
        if range:
            return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=range))
        else:
            return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=10))
    elif data_type.lower() == "date":
        if range:
            start_date, end_date = range
            return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        else:
            return date.today()
    elif data_type.lower().startswith("decimal"):
        if range:
            return Decimal(random.randint(range[0], range[1]))
        else:
            return Decimal(random.randint(0, 1000))
    elif data_type.lower() == "boolean":  # Handle boolean type
        return random.choice([True, False])
    else:
        return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=10))



if __name__ == "__main__":
    spark = SparkSession.builder.appName("GenerateRandomData").enableHiveSupport().getOrCreate()
    table_names = ["table1", "table2"]
    # Specify ranges for specific columns in each table
    ranges = {
        "table1": {
            "age": (18, 65), 
            "id": (100, 1000),
            "price": (0.5, 100.0),  # Double range
            "created_at": (datetime(2022, 1, 1), datetime(2023, 1, 1)),  # Timestamp range
            "small_value": (0, 10),  # Smallint range
            "tiny_value": (0, 255),  # Tinyint range
            "name": 20,  # Char/Varchar length
            "description": 100, # Char/Varchar length
            "date_joined": (datetime(2022, 1, 1), datetime(2023, 1, 1)),  # Date range
            "decimal_value": (0.01, 1000.00)  # Decimal range
        },
        "table2": {
            "col1": (0, 100),
            "col2": datetime(2023, 1, 1)
        }
    }
    # Define foreign key relationships (child, parent)
    foreign_keys = {
        ("table2", "customer_id"): ("table1", "customer_id")
    }
    # Define possible string values for specific columns
    string_values = {
        "table1": {
            "status": ["active", "inactive"]
        }
    }
    #generate_random_data(spark, table_names, ranges=ranges, foreign_keys=foreign_keys, string_values=string_values)

    # generate_random_data(spark=spark, 
    #                      table_names=["transactions"], 
    #                      num_records=10, 
    #                      ranges={
    #                                 "transactions": 
    #                                 {
    #                                     "submissiondate": (datetime(2022, 1, 1), datetime(2023, 1, 1))
    #                                 }
    #                             }, 
    #                      foreign_keys={}, 
    #                      string_values={
    #                                         "transactions": 
    #                                         {
    #                                             "transactiontype": ["accepted", "declined"]
    #                                         }
    #                                     }
    #                     )
    
    generate_random_data(
                            spark=spark, 
                            table_names=["t1", "t2"], 
                            num_records=10,
                            foreign_keys = {
                                ("t2", "id"): ("t1", "id")
                            }
                        )
    spark.stop()

