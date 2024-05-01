from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.api_core.exceptions import AlreadyExists

# Write some friendly greetings to Cloud Bigtable
GREETINGS = [
    "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
]

# Connects to Cloud Bigtable, runs some basic operations and prints the results.
def do_hello_world(project_id, instance_id):
    # Connect to an existing instance
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    # Create a table with a single column family
    table_id = "your-table-id"
    table = instance.table(table_id)
    column_family_id = "cf1"
    column_family = table.column_family(column_family_id)
    # Verificar si la tabla existe
    if not instance.table(table_id).exists():
        print(f"Creating table {table_id}.")
        table.create()

    # Verificar si la familia de columnas existe
    try:
    # Verificar si la familia de columnas existe y crearla si no
        existing_column_families = table.list_column_families()
        if column_family_id.encode() not in existing_column_families:
            print(f"Creating column family {column_family_id}.")
            column_family.create()
    except AlreadyExists:
        print(f"Column family {column_family_id} already exists.")


    # Write some rows to the table
    # Escribir algunas filas en la tabla
    print("Escribiendo saludos en la tabla")
    
    rows_to_mutate = []

    for i, greeting in enumerate(GREETINGS):
        row_key = f"greeting{i}"
        row = table.row(row_key)
        row.set_cell(column_family_id, b"greeting", greeting)
        rows_to_mutate.append(row)

    # Enviamos las mutaciones a la tabla en un solo lote
    for row in rows_to_mutate:
        row.commit()


    # Get the first greeting by row key
    row_key = b"greeting0"
    row = table.read_row(row_key)
    greeting = row.cells[column_family_id][b"greeting"][0].value.decode("utf-8")
    print("Get a single greeting by row key")
    print(f"\t{row_key.decode('utf-8')} = {greeting}")

    # Scan across all rows
    print("Scan for all greetings:")
    partial_rows = table.read_rows()
    for row in partial_rows:
        for cf, cols in row.cells.items():
            for col, cells in cols.items():
                for cell in cells:
                    print(f"\t{cell.value.decode('utf-8')}")

    # Clean up by deleting the table
    #print("Delete the table")
    #table.delete()
    #print(f"Table {table_id} deleted.")


def main():
    # Consult environment variables to get project/instance
    import os
    project_id = "valiant-ocean-422009-q5"
    instance_id = "hola-mundo"

    if not project_id or not instance_id:
        raise ValueError("Missing required environment variables: BIGTABLE_PROJECT_ID or BIGTABLE_INSTANCE_ID")

    do_hello_world(project_id, instance_id)


if __name__ == "__main__":
    main()
