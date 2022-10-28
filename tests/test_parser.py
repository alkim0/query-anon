from pathlib import Path

import pytest

from query_anon.log_reader import SingleLogReader
from query_anon.parser import QueryParser

DATA_PATH = Path(__file__).parent / "data"


@pytest.fixture
def tpc_h_log_path() -> Path:
    return DATA_PATH / "tpc-h.log"


def test_parser(tpc_h_log_path) -> None:
    log_reader = SingleLogReader(tpc_h_log_path)
    log_reader.read()
    queries = log_reader.get_queries()
    queries = QueryParser().parse_queries(queries)
    queries = [q.lower() for q in queries]
    assert queries == [
        "select a0, a1, sum(a2) as aa0, sum(a3) as aa1, sum(a3 * (1 - a4)) as aa2, sum(a3 * (1 - a4) * (1 + a6)) as aa3, avg(a2) as aa4, avg(a3) as aa5, avg(a4) as aa6, count(*) as aa7 from t0 where a5 <= cast('x' as date) - interval 'x' v0 group by a0, a1 order by a0, a1",
        "select a17, a18, a19, a13, a20, a21, a22, a23 from t5, t2, t1, t3, t4 where a13 = a14 and a15 = a16 and a25 = 1 and a24 like 'x' and a11 = a12 and a9 = a10 and a8 = 'x' and a7 = (select min(a7) from t1, t2, t3, t4 where a13 = a14 and a15 = a16 and a11 = a12 and a9 = a10 and a8 = 'x') order by a17 desc, a19, a18, a13",
        "select a26, sum(a3 * (1 - a4)) as aa0, a27, a28 from t6, t7, t0 where a30 = 'x' and a31 = a32 and a26 = a29 and a27 < cast('x' as date) and a5 > cast('x' as date) group by a26, a27, a28 order by aa0 desc, a27",
        "select a33, count(*) as aa0 from t7 where a27 >= cast('x' as date) and a27 < cast('x' as date) + interval 'x' v1 and exists(select from t0 where a26 = a29 and a34 < a35) group by a33 order by a33",
        "select a19, sum(a3 * (1 - a4)) as aa0 from t6, t7, t0, t2, t3, t4 where a31 = a32 and a26 = a29 and a37 = a15 and a36 = a11 and a11 = a12 and a9 = a10 and a8 = 'x' and a27 >= cast('x' as date) and a27 < cast('x' as date) + interval 'x' v2 group by a19 order by aa0 desc",
        "select sum(a3 * a4) as aa0 from t0 where a5 >= cast('x' as date) and a5 < cast('x' as date) + interval 'x' v2 and a4 between 1 - 1 and 1 + 1 and a2 < 1",
        "select a38, a39, a40, sum(a41) as aa0 from (select ta1.a42 as aa0, ta2.a42 as aa1, extract(v3 from a43) as aa2, a44 * (1 - a45) as aa3 from t8, t9, t10, t11, t12 as ta1, t12 as ta2 where a51 = a52 and a53 = a54 and a49 = a50 and a48 = ta1.a47 and a46 = ta2.a47 and ((ta1.a42 = 'x' and ta2.a42 = 'x') or (ta1.a42 = 'x' and ta2.a42 = 'x')) and a43 between cast('x' as date) and cast('x' as date)) as ta0 group by a38, a39, a40 order by a38, a39, a40",
        "select a56, sum(case when a58 = 'x' then a41 else 1 end) / sum(a41) as aa0 from (select extract(v3 from a57) as aa0, a44 * (1 - a45) as aa1, ta2.a42 as aa2 from t13, t8, t9, t10, t11, t12 as ta1, t12 as ta2, t14 where a63 = a64 and a51 = a52 and a54 = a53 and a50 = a49 and a46 = ta1.a47 and ta1.a61 = a62 and a60 = 'x' and a48 = ta2.a47 and a57 between cast('x' as date) and cast('x' as date) and a59 = 'x') as ta0 group by a56 order by a56",
        "select a58, a56, sum(a66) as aa0 from (select a42 as aa0, extract(v3 from a57) as aa1, a44 * (1 - a45) - a67 * a68 as aa2 from t13, t8, t9, t15, t10, t12 where a51 = a52 and a71 = a52 and a70 = a64 and a63 = a64 and a53 = a54 and a48 = a47 and a69 like 'x') as ta0 group by a58, a56 order by a58, a56 desc",
        "select a31, a72, sum(a3 * (1 - a4)) as aa0, a73, a19, a74, a75, a76 from t6, t7, t0, t3 where a31 = a32 and a26 = a29 and a27 >= cast('x' as date) and a27 < cast('x' as date) + interval 'x' v1 and a0 = 'x' and a36 = a12 group by a31, a72, a73, a75, a19, a74, a76 order by aa0 desc",
        "select a14, sum(a7 * a77) as aa0 from t1, t2, t3 where a16 = a15 and a11 = a12 and a19 = 'x' group by a14 having sum(a7 * a77) > (select sum(a7 * a77) * 1 from t1, t2, t3 where a16 = a15 and a11 = a12 and a19 = 'x') order by aa0 desc",
        "select a78, sum(case when a33 = 'x' or a33 = 'x' then 1 else 1 end) as aa0, sum(case when a33 <> 'x' and a33 <> 'x' then 1 else 1 end) as aa1 from t7, t0 where a29 = a26 and a78 in ('x', 'x') and a34 < a35 and a5 < a34 and a35 >= cast('x' as date) and a35 < cast('x' as date) + interval 'x' v2 group by a78 order by a78",
        "select a80, count(*) as aa2 from (select aa0, count(a53) from t11 left outer join t10 on aa0 = a50 and not a81 like 'x' group by aa0) as ta0(aa0, aa1) group by a80 order by aa2 desc, a80 desc",
        "select 1 * sum(case when a24 like 'x' then a3 * (1 - a4) else 1 end) / sum(a3 * (1 - a4)) as aa0 from t0, t5 where a55 = a13 and a5 >= cast('x' as date) and a5 < cast('x' as date) + interval 'x' v1",
        "select a15, a18, a21, a22, a82 from t2, t16 as ta0 where a15 = a83 and a82 = (select max(a82) from t16 as ta0) order by a15",
        "select a84, a24, a25, count(distinct a16) as aa0 from t1, t5 where a13 = a14 and a84 <> 'x' and not a24 like 'x' and a25 in (1, 1, 1, 1, 1, 1, 1, 1) and not a16 in (select a15 from t2 where a23 like 'x') group by a84, a24, a25 order by aa0 desc, a84, a24, a25",
        "select sum(a3) / 1 as aa0 from t0, t5 where a13 = a55 and a84 = 'x' and a85 = 'x' and a2 < (select 1 * avg(a2) from t0 where a55 = a13)",
        "select a72, a31, a29, a27, a86, sum(a2) from t6, t7, t0 where a29 in (select a26 from t0 group by a26 having sum(a2) > 1) and a31 = a32 and a29 = a26 group by a72, a31, a29, a27, a86 order by a86 desc, a27",
        "select sum(a3 * (1 - a4)) as aa0 from t0, t5 where (a13 = a55 and a84 = 'x' and a85 in ('x', 'x', 'x', 'x') and a2 >= 1 and a2 <= 1 + 1 and a25 between 1 and 1 and a78 in ('x', 'x') and a87 = 'x') or (a13 = a55 and a84 = 'x' and a85 in ('x', 'x', 'x', 'x') and a2 >= 1 and a2 <= 1 + 1 and a25 between 1 and 1 and a78 in ('x', 'x') and a87 = 'x') or (a13 = a55 and a84 = 'x' and a85 in ('x', 'x', 'x', 'x') and a2 >= 1 and a2 <= 1 + 1 and a25 between 1 and 1 and a78 in ('x', 'x') and a87 = 'x')",
        "select a18, a21 from t2, t3 where a15 in (select a16 from t1 where a14 in (select a13 from t5 where a65 like 'x') and a77 > (select 1 * sum(a68) from t9 where a64 = a14 and a52 = a16 and a43 >= cast('x' as date) and a43 < cast('x' as date) + interval 'x' v3)) and a11 = a12 and a19 = 'x' order by a18",
        "select a18, count(*) as aa0 from t2, t0 as ta0, t7, t3 where a15 = ta0.a37 and a29 = ta0.a26 and a88 = 'x' and ta0.a35 > ta0.a34 and exists(select * from t0 as ta1 where ta1.a26 = ta0.a26 and ta1.a37 <> ta0.a37) and not exists(select * from t0 as ta2 where ta2.a26 = ta0.a26 and ta2.a37 <> ta0.a37 and ta2.a35 > ta2.a34) and a11 = a12 and a19 = 'x' group by a18 order by aa0 desc, a18",
        "select a89, count(*) as aa0, sum(a73) as aa1 from (select substring(a90, 1, 1) as aa0, a73 from t11 where substring(a90, 1, 1) in ('x', 'x', 'x', 'x', 'x', 'x', 'x') and a73 > (select avg(a73) from t11 where a73 > 1 and substring(a90, 1, 1) in ('x', 'x', 'x', 'x', 'x', 'x', 'x')) and not exists(select * from t10 where a50 = a49)) as ta0 group by a89 order by a89",
    ]
