import streamlit as st
import pandas as pd
import urllib.parse

BASE_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.csv"
START_YEAR = 2018
END_YEAR = 2024

def clean_eda_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["hour"] = df["date"].dt.hour
    df["dow"] = df["date"].dt.dayofweek  

    if "primary_type" in df.columns:
        df["primary_type"] = df["primary_type"].astype(str).str.strip().str.upper()
        df.loc[df["primary_type"].isin(["", "NONE", "NAN"]), "primary_type"] = pd.NA

    if "latitude" in df.columns and "longitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        df = df[
            df["latitude"].between(41.0, 42.2)
            & df["longitude"].between(-88.5, -87.0)
        ]


    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])

    return df

st.set_page_config(page_title="IT5006 Phase 1 EDA", layout="wide")
st.title("IT5006 – Phase 1 EDA")


def build_url(params: dict) -> str:
    return BASE_URL + "?" + urllib.parse.urlencode(params)


@st.cache_data(show_spinner=True)
def load_yearly_counts():
    results = {}
    for y in range(START_YEAR, END_YEAR + 1):
        params = {
            "$select": "count(*) as cnt",
            "$where": f"date between '{y}-01-01T00:00:00' and '{y}-12-31T23:59:59'",
        }
        url = build_url(params)
        tmp = pd.read_csv(url)
        results[y] = int(pd.to_numeric(tmp.loc[0, "cnt"], errors="coerce"))
    return pd.Series(results).sort_index()


@st.cache_data(show_spinner=True)
def load_monthly_counts(year: int):
    results = {}
    for m in range(1, 13):
        start = f"{year}-{m:02d}-01T00:00:00"
        if m == 12:
            end = f"{year+1}-01-01T00:00:00"
        else:
            end = f"{year}-{m+1:02d}-01T00:00:00"

        params = {
            "$select": "count(*) as cnt",
            "$where": f"date >= '{start}' and date < '{end}'",
        }
        url = build_url(params)
        tmp = pd.read_csv(url)
        results[m] = int(pd.to_numeric(tmp.loc[0, "cnt"], errors="coerce"))

    return pd.Series(results).sort_index()

@st.cache_data(show_spinner=True)
def load_top_types(year: int, top_n: int = 10):
    params = {
        "$select": "primary_type, count(*) as cnt",
        "$where": f"date between '{year}-01-01T00:00:00' and '{year}-12-31T23:59:59'",
        "$group": "primary_type",
        "$order": "cnt desc",
        "$limit": top_n,
    }
    url = build_url(params)
    df = pd.read_csv(url)
    df["cnt"] = pd.to_numeric(df["cnt"], errors="coerce")
    return df, url

@st.cache_data(show_spinner=True)
def load_map_sample(year: int, limit: int = 20000):
    params = {
        "$select": "date,primary_type,latitude,longitude",
        "$where": (
            f"date between '{year}-01-01T00:00:00' and '{year}-12-31T23:59:59' "
            "AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ),
        "$order": "date desc",
        "$limit": int(limit),
    }
    url = build_url(params)
    df = pd.read_csv(url)
    df = clean_eda_df(df)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
    return df, url

@st.cache_data(show_spinner=True)
def load_year_type_counts(start_year=2018, end_year=2024, top_n=5):
    # 1) 先拿 Top N 类型（全时间段，真实count，不抽样）
    params_top = {
        "$select": "primary_type, count(*) as cnt",
        "$where": f"date between '{start_year}-01-01T00:00:00' and '{end_year}-12-31T23:59:59'",
        "$group": "primary_type",
        "$order": "cnt DESC",
        "$limit": int(top_n),
    }
    url_top = build_url(params_top)
    top_df = pd.read_csv(url_top)
    top_types = top_df["primary_type"].dropna().tolist()

    if len(top_types) == 0:
        return pd.DataFrame(), url_top

    # 2) 再按 年份 + 类型 聚合（关键：date_extract_y）
    type_filter = ", ".join([f"'{t}'" for t in top_types])
    year_expr = "date_extract_y(date)"

    params_trend = {
        "$select": f"{year_expr} as year, primary_type, count(*) as cnt",
        "$where": (
            f"date between '{start_year}-01-01T00:00:00' and '{end_year}-12-31T23:59:59' "
            f"AND primary_type IN ({type_filter})"
        ),
        # 注意：group 里必须写表达式本体，不要写 year 这个别名
        "$group": f"{year_expr}, primary_type",
        "$order": f"{year_expr} ASC",
    }
    url_trend = build_url(params_trend)

    df = pd.read_csv(url_trend)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["cnt"] = pd.to_numeric(df["cnt"], errors="coerce")

    df = df.dropna(subset=["year", "primary_type", "cnt"])
    df["year"] = df["year"].astype(int)
    return df, url_trend
# Sidebar
st.sidebar.header("Controls")
year_choice = st.sidebar.selectbox("Choose year (for monthly/type/map)", list(range(END_YEAR, START_YEAR - 1, -1)))
top_n = st.sidebar.slider("Top N crime types", 5, 20, 10)
map_limit = st.sidebar.slider("Map sample limit", 5000, 50000, 20000, step=5000)
map_points = st.sidebar.slider("Max points plotted on map", 1000, 20000, 5000, step=1000)
show_debug = st.sidebar.checkbox("Show debug URLs", value=False)

# 1) Yearly Trend (TRUE)
st.subheader("1) Temporal Pattern: Crimes by Year (TRUE Population Trend)")
yearly = load_yearly_counts()
st.line_chart(yearly)


with st.expander("Show yearly counts table"):
    st.dataframe(yearly.rename("count").reset_index().rename(columns={"index": "year"}))

# 2) Monthly Trend
st.subheader(f"2) Monthly Pattern: Crimes by Month ({year_choice})")
monthly = load_monthly_counts(year_choice)
st.line_chart(monthly)

with st.expander("Show monthly counts table"):
    st.dataframe(monthly.rename("count").reset_index().rename(columns={"index": "month"}))

# 3) Top Crime Types
st.subheader(f"3) Crime Type Distribution: Top {top_n} Primary Types ({year_choice})")
top_types_df, top_types_url = load_top_types(year_choice, top_n=top_n)


if not top_types_df.empty:
    chart_df = top_types_df.set_index("primary_type")["cnt"]
    st.bar_chart(chart_df)
    st.dataframe(top_types_df)
else:
    st.warning("No type data returned (unexpected).")

st.subheader("Crime Correlation: Crime Type vs Year")

type_trend_df, type_trend_url = load_year_type_counts(2018, 2024, 5)

if type_trend_df.empty:
    st.warning("No data returned for crime type trend.")
else:
    pivot_df = (
        type_trend_df
        .pivot(index="year", columns="primary_type", values="cnt")
        .fillna(0)
        .sort_index()
    )
    st.line_chart(pivot_df)

    if show_debug:
        st.caption("DEBUG – API URL")
        st.code(type_trend_url)
        st.write(pivot_df)

# 4) Map

st.subheader(f"4) Spatial Distribution Map (Sample) — {year_choice}")
map_df, map_url = load_map_sample(year_choice, limit=map_limit)

if len(map_df) == 0:
    st.warning("No map points returned. Try increasing map_limit or changing year.")
else:
    if len(map_df) > map_points:
        map_df = map_df.sample(map_points, random_state=42)
    st.map(map_df[["lat", "lon"]])
    st.caption(f"Map points shown: {len(map_df):,}")



if show_debug:
    st.markdown("---")
    st.subheader("Debug: API URLs")
    st.write("Top Types URL:")
    st.code(top_types_url)
    st.write("Map URL:")
    st.code(map_url)

st.markdown("---")
st.caption(
    "Note: Yearly & monthly trends use count(*) aggregation (true population trend, no sampling). "
    "Map uses sampling for performance."
)
