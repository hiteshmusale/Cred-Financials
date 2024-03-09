import happybase
import random
import time

def connect_to_hbase():
    return happybase.Connection('localhost', port=9090)

def read_random_rows(connection, table_name):
    try:
        # Open the table
        table = connection.table(table_name)

        # Scan the table to retrieve all rows
        scanner = table.scan()

        # Convert the scanner object to a list of rows
        all_rows = list(scanner)

        # Shuffle the rows to make them random
        random.shuffle(all_rows)

        # Select five random rows from the shuffled list
        random_rows = random.sample(all_rows, min(5, len(all_rows)))

        # Print the selected random rows
        for row_key, data in random_rows:
            print(f'Row Key: {row_key}')
            for column, value in data.items():
                print(f'{column}: {value}')
            print('---')

    except happybase.TTransportException as e:
        print(f"Error: {e}. Retrying in 5 seconds...")
        time.sleep(5)
        read_random_rows(connect_to_hbase(), table_name)

# Specify the table name
table_name = 'card_member'

# Retry connecting and reading up to 3 times
for _ in range(3):
    try:
        # Connect to HBase
        connection = connect_to_hbase()

        # Read random rows
        read_random_rows(connection, table_name)

        # Close the connection
        connection.close()
        break  # If successful, break out of retry loop
    except Exception as e:
        print(f"Error: {e}. Retrying...")
