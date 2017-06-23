# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
from pymapd import connect

drop = 'DROP TABLE IF EXISTS stocks;'
create = '''\
CREATE TABLE stocks (
  date_ text,
  trans text,
  symbol text,
  qty int,
  price float,
  vol float);'''


class TimeSuite:
    """
    An example benchmark that times the performance of various kinds
    of iterating over dictionaries in Python.
    """
    def setup(self):
        self.con = connect(user='mapd', password='HyperInteractive',
                           host='localhost', dbname="mapd")
        self.cursor = self.con.cursor()
        self.cursor.execute(drop)
        self.cursor.execute(create)
        self.cursor.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14,1.1);")  # noqa
        self.cursor.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','GOOG',100,12.14,1.2);")  # noqa

    def time_select_small(self):
        self.cursor.execute("select * from stocks;")

    def time_select_small_to_list(self):
        list(self.cursor.execute("select * from stocks;"))

    def time_select_all_flights_2008_10k(self):
        self.cursor.execute('select * from flights_2008_10k')

    def time_select_numeric_flights_2008_10k(self):
        self.cursor.execute("""\
            select flight_year, flight_month, deptime, arrtime, \
            airtime, arrdelay, depdelay from flights_2008_10k""")
