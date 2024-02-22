# DATASCOPE Interface

This repository is a heavily modified version of a program originally written by Erfan Danesh. Originally intended to query futures data day by day, this version of the code imposes a structure which allows for many asset classes and report structures. In short, it's a far more general and user friendly experience that encourages customization for user defined security types.

## Setting User Information

Before interfacing with Datascope, we must first initialize their user information. To run the program as is, replace `USERNAME` and `PASSWORD` with the appropriate credentials and call `download.py` in the terminal. Optionally a proxy can be set by replacing both fields of `PROXY` at the top of the download script.

Alternatively, to call the program in a notebook or in another file you can initialize like so:

```python
# replace the following with your own credentials
auth = Session(USERNAME,PASSWORD)
```

## Requesting and Downloading

Built in, is a custom interface for designing reports given an asset class. Suppose we want to extract trade data for a given security, at the ticker level. There are 3 major elements to designing an extraction: (1) a security, (2) a report type and (3) a date range.

Securities are represented by the eponymous super class, `Security`. This class is designed only as an umbrella to unify the various asset classes which are used to generate a report. For example, one can call `Futures(id,tz)` to specify an asset defined by some identifier `id` (usually the RIC), at an exchange located in timezone `tz`. While it seems redundant, the inclusion of the timezone remedies the various problems such as daylight savings and exchange time discrepencies.

```python
# e-mini S&P 500 futures exchanged by CME
asset = Futures("ES","US/Central")
eod_report = EndOfDay(auth,asset)

# requests occur in 4 day intervals 
serial_download(eod_report,"2024-01-01","2024-02-01",num_dates=4)
```

Some combinations of RICs and asset classes aren't necessarily compatible; and while error handling does a good enough job, there are instances which slip through the cracks. Regardless the user can specify any of the following securities and report types below.

**Securities** (using base RIC as the identifier)
- Futures
- Options
- Equity
- Treasury
- Fixed Income


**Report Types**
- End of Day
- Intra-Day
- High Frequency (Trades, Quotes and Depths)

#### Parallel Requests

The most efficient way to use this program is via the `parallel_download` function. The user specifies the asset, report type, and date range as before but with an additional parameter `num_procs`.

```python
trades = Trades(auth,Futures("FF","US/Central"))
parallel_download(
    trades,
    "2024-01-01",
    "2024-02-01",
    num_procs = 20,
    num_dates = 4
)
```

Most of the changes made to this code base were to support a more efficient means of requesting in parallel. While it's highly efficient, the API limits the user to roughly 200 calls over a 60s interval. Further constraints occur when making too many requests in a similar interval, even if the number of processes is set to a reasonable number like 30, if sufficiently quick these requests hit a maximum and timeout the user.

In general, if the number of processes is set too high, things may break down. Even though error handling should catch these instances, they may result in idled processes in specific workers.

## Contact

While I still have access to a Refinitiv account, feel free to submit an issue or reach out over email (charles.a.knipp@frb.gov)

*Beliefs held by the authors of this code base do not reflect those of the Federal Reserve Board of Governors or those of the greater Federal Reserve System. Any and all contributions are thus independent of the Board and it's functions.*