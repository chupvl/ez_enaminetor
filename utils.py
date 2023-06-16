def read_ipc(filename):
    #f = '/home/chupvl/PycharmProjects/ezenaminetor/tmp_ll_9.txt.ipc'
    #tmp = pa.ipc.RecordBatchFileReader(filename)
    ipc_file = pa.ipc.open_file(filename)
    schema = ipc_file.schema
    print(schema)
    table = ipc_file.read_all()
    print(table)