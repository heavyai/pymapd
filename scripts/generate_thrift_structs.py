import os
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from mapd import MapD

HERE = os.path.dirname(__file__)
DEST = os.path.join(os.path.dirname(HERE), "tests", "data")


def get_client(host_or_uri, port):
    socket = TSocket.TSocket(host_or_uri, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = MapD.Client(protocol)
    transport.open()
    return client


def main():

    db_name = 'mapd'
    user_name = 'mapd'
    passwd = 'HyperInteractive'
    hostname = 'localhost'
    portno = 9091

    print("Connecting")
    client = get_client(hostname, portno)
    session = client.connect(user_name, passwd, db_name)

    print("Creating table `stocks`")
    drop = 'drop table if exists stocks;'
    client.sql_execute(session, drop, True, None, -1)
    create = ('create table stocks (date_ text, trans text, symbol text, '
              'qty int, price float, vol float);')
    client.sql_execute(session, create, True, None, -1)

    print("Inserting data")
    i1 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);"
    i2 = "INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);"
    client.sql_execute(session, i1, True, None, -1)
    client.sql_execute(session, i2, True, None, -1)
    select = "select * from stocks;"
    colwise = client.sql_execute(session, select, True, None, -1)
    rowwise = client.sql_execute(session, select, False, None, -1)

    with open(os.path.join(DEST, "rowwise.py"), 'w') as f:
        f.write(str(rowwise))

    with open(os.path.join(DEST, "colwise.py"), 'w') as f:
        f.write(str(colwise))

    client.disconnect(session)


if __name__ == '__main__':
    main()
