`kofu-hourly.html` contains the heading and hourly table from JMA's public
[Kofu observations for 2026-10-24](https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php?prec_no=49&block_no=47638&year=2026&month=10&day=24&view=p1),
retrieved on 2026-10-25. Navigation and unrelated page content were removed.
Source: Japan Meteorological Agency. This is a parser fixture, not a current
weather feed.

The following fixtures likewise retain only the public page heading and table,
retrieved on 2026-10-25 from the Japan Meteorological Agency:

- `katsunuma-hourly.html`: [Katsunuma, 2026-10-24](https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php?prec_no=49&block_no=0433&year=2026&month=10&day=24&view=p1).
- `katsunuma-2020-hourly.html`: [Katsunuma, 2020-10-24](https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php?prec_no=49&block_no=0433&year=2020&month=10&day=24&view=p1), before the sunshine-estimation transition.
- `uenohara-hourly.html`: [Uenohara, 2026-10-24](https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php?prec_no=49&block_no=1165&year=2026&month=10&day=24&view=p1), a rain-only station.

Tests that alter dates, cells, or headers do so in memory; they do not represent
those mutations as actual observations on the substituted date.
