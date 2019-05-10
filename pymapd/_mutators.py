"""
Setter and Getter for TDataFrame
"""


def set_tdf(self, tdf):
    """Assigns a TDataFrame to cudf/pandas Dataframe

        Parameters
        ----------
        tdf: TDataFrame
            A SQL select statement

        Example
        -------
        >>> df.set_tdf(tdf)
        """
    self._tdf = tdf


def get_tdf(self):
    """Returns assigned TDataFrame.

        Example
        -------
        >>> df.get_tdf()
        TDataFrame(sm_handle=b'\xa30q%', sm_size=632, df_handle=b'\x90a>
        \x06\x00\x00\x00\x00\xe0\xe6\x00\x00\x00\x00\x00\x00\xe0|E\x00\x00
        \x00\xca\x01\xd0\xc1"\x03\x00\\', df_size=4553952)
        """

    return self._tdf
