import pyarrow as pa

def read_ipc(filename):
    ipc_file = pa.ipc.open_file(filename)
    schema = ipc_file.schema
    print(schema)
    table = ipc_file.read_all()
    print(table)