"""
27. Universal File Ingestion Framework -> Data Lake Hive Tables
===============================================================
Azure Data Engineering Product | PySpark

One function per source-file category. Every category function takes a
`file_type` argument, reads the source with the right Spark/native reader
(or a documented fallback), and writes the result to a Hive table whose
data lives on the data lake.

Categories (one function each):
  1.  ingest_structured_data_files        (CSV/TSV/delimited/Excel/ODS/...)
  2.  ingest_semi_structured_data_files   (JSON/XML/YAML/Avro/...)
  3.  ingest_bigdata_analytics_files      (Parquet/ORC/Delta/Hudi/TFRecord/...)
  4.  ingest_database_export_files        (SQL dumps/SQLite/BCP/DAT/...)
  5.  ingest_log_event_files              (Apache/Nginx/W3C/ELF/JSON logs/...)
  6.  ingest_compressed_archived_files    (ZIP/GZIP/TAR/7z/RAR/...)
  7.  ingest_text_document_files           (TXT/RTF/DOC/DOCX/PDF/Markdown/...)
  8.  ingest_image_files                  (JPEG/PNG/GIF/TIFF/WebP/...)
  9.  ingest_audio_files                  (WAV/MP3/AAC/FLAC/...)
  10. ingest_video_files                  (MP4/MOV/AVI/MKV/...)
  11. ingest_geospatial_gis_files         (Shapefile/GeoJSON/KML/GeoTIFF/...)
  12. ingest_scientific_engineering_files (HDF5/NetCDF/MAT/FITS/NPY/SAS/...)
  13. ingest_cloud_datalake_objects       (S3/ADLS/GCS objects, any format)
  14. ingest_config_metadata_files        (JSON/YAML/TOML/INI/ENV/Proto/...)
  15. ingest_api_exchange_files           (JSON/XML/Avro/Protobuf/MsgPack/...)
  16. ingest_streaming_messaging_files    (JSON/Avro/Thrift/CBOR/BSON/...)
  17. ingest_ml_ai_data_files             (LibSVM/ARFF/YOLO/COCO/NPY/...)
  18. ingest_enterprise_export_files      (EDI/X12/EDIFACT/IDoc/SAP/...)
  19. ingest_recommended_source_files     (curated product-ready subset)
  20. data_engineering_operations         (21 ops: validation, DQ, lineage...)

Design notes:
  - Binary/media files land as FILE CATALOG tables (path, size, format,
    timestamps + extracted metadata where cheap) - the standard lake
    pattern for non-tabular data.
  - Readers Spark cannot do natively fall back to driver-side pandas/python
    libraries with a clear error if the library is missing.
  - All secrets via env vars; no credentials in code.
"""

import os
import re
import io
import json
import shutil
import tempfile
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# ----------------------------- Configuration -----------------------------
CONFIG = {
    "lake_base_path": os.environ.get(
        "LAKE_BASE_PATH", "abfss://lake@<storage-account>.dfs.core.windows.net"
    ),
    "hive_db": os.environ.get("HIVE_DB", "lake"),
    "landing_zone": os.environ.get(
        "LANDING_ZONE", "abfss://landing@<storage-account>.dfs.core.windows.net"
    ),
    "audit_table": os.environ.get("AUDIT_TABLE", "ops.ingestion_audit"),
    "lineage_table": os.environ.get("LINEAGE_TABLE", "ops.data_lineage"),
}


# ----------------------------- Spark session -----------------------------
def get_spark(app_name: str = "file-ingestion-framework") -> SparkSession:
    """Create (or reuse) a Hive-enabled Spark session on the data lake."""
    try:
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.warehouse.dir", f"{CONFIG['lake_base_path']}/warehouse")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )


# ----------------------------- Small helpers -----------------------------
def normalize_file_type(file_type: str) -> str:
    """'CSV' / '.csv' / 'tar.gz' -> canonical key used by the dispatch."""
    ft = file_type.strip().lower().lstrip(".")
    return COMPOUND_ALIASES.get(ft, ft)


COMPOUND_ALIASES = {
    "tgz": "tar.gz",
    "tbz2": "tar.bz2",
    "ndjson": "jsonl",
    "htm": "html",
    "yml": "yaml",
    "jpeg": "jpg",
    "tif": "tiff",
    "aif": "aiff",
    "mpg": "mpeg",
    "sqlite3": "sqlite",
    "msgpack": "msgpack",
    "mpk": "msgpack",
    "feather": "arrow",
}


def detect_file_type(path: str) -> str:
    """Operation: file format detection from the file name/extension."""
    name = path.rstrip("/").split("/")[-1].lower()
    for compound in ("tar.gz", "tar.bz2", "tar.xz"):
        if name.endswith("." + compound):
            return compound
    if "." not in name:
        return "unknown"
    return normalize_file_type(name.rsplit(".", 1)[-1])


def sanitize_column(name: str) -> str:
    clean = re.sub(r"[^0-9a-zA-Z_]+", "_", name).lower().strip("_")
    if re.match(r"^[0-9]", clean):
        clean = f"c_{clean}"
    return clean or "col"


def parse_table_name(hive_table: str) -> Tuple[str, str]:
    if "." in hive_table:
        return hive_table.split(".", 1)
    return CONFIG["hive_db"], hive_table


def _pandas_to_df(spark: SparkSession, pdf, table_hint: str = "") -> DataFrame:
    """Driver-side pandas -> Spark DataFrame with sanitized columns."""
    pdf = pdf.copy()
    pdf.columns = [sanitize_column(str(c)) for c in pdf.columns]
    return spark.createDataFrame(pdf)


# ============================ FORMAT READERS =============================
# Each reader: read_source_file(spark, path, file_type, **options) -> DataFrame

def _read_delimited(
    spark: SparkSession, path: str, sep: str = ",", **options
) -> DataFrame:
    return (
        spark.read.option("header", options.get("header", "true"))
        .option("inferSchema", options.get("inferSchema", "true"))
        .option("sep", sep)
        .option("multiLine", options.get("multiLine", "true"))
        .option("escape", options.get("escape", '"'))
        .csv(path)
    )


def _read_fixed_width(spark: SparkSession, path: str, **options) -> DataFrame:
    """Fixed-width: options widths=[...], columns=[...]."""
    widths: List[int] = options.get("widths") or []
    columns: List[str] = options.get("columns") or []
    if not widths or not columns or len(widths) != len(columns):
        raise ValueError("fixed-width needs options widths=[...] and columns=[...]")
    lines = spark.read.text(path).withColumnRenamed("value", "line")
    pos, exprs = 1, []
    for w, c in zip(widths, columns):
        exprs.append(F.substring(F.col("line"), pos, w).alias(sanitize_column(c)))
        pos += w
    return lines.select(*exprs)


def _read_excel(spark: SparkSession, path: str, **options) -> DataFrame:
    try:  # spark-excel package when available
        return (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dataAddress", options.get("dataAddress", "0!A1"))
            .load(path)
        )
    except Exception:
        import pandas as pd  # fallback for small files

        pdf = pd.read_excel(path, sheet_name=options.get("sheet", 0))
        return _pandas_to_df(spark, pdf)


def _read_json(spark: SparkSession, path: str, **options) -> DataFrame:
    multi = options.get("multiLine", "false" if path.endswith("l") else "true")
    return spark.read.option("multiLine", multi).json(path)


def _read_json5(spark: SparkSession, path: str, **options) -> DataFrame:
    import json5  # pip install json5

    sc = spark.sparkContext
    raw = sc.textFile(path).collect()
    docs = [json5.loads(r) for r in raw if r.strip()]
    import pandas as pd

    return _pandas_to_df(spark, pd.json_normalize(docs))


def _read_xml(spark: SparkSession, path: str, **options) -> DataFrame:
    try:  # spark-xml package when available
        return (
            spark.read.format("com.databricks.spark.xml")
            .option("rowTag", options.get("rowTag", "row"))
            .option("inferSchema", "true")
            .load(path)
        )
    except Exception:
        return spark.read.text(path).withColumnRenamed("value", "raw_xml")


def _read_yaml(spark: SparkSession, path: str, **options) -> DataFrame:
    import yaml
    import pandas as pd

    sc = spark.sparkContext
    docs = [yaml.safe_load(t) for t in sc.textFile(path).collect() if t.strip()]
    flat = [d if isinstance(d, dict) else {"value": d} for d in docs]
    return _pandas_to_df(spark, pd.json_normalize(flat))


def _read_text_lines(spark: SparkSession, path: str, **options) -> DataFrame:
    return spark.read.text(path).withColumnRenamed("value", "line")


def _read_binary_meta(
    spark: SparkSession, path: str, file_type: str, **options
) -> DataFrame:
    """File-catalog pattern for binary/media: one row per file."""
    return (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(path)
        .select(
            F.col("path").alias("file_path"),
            F.length("content").alias("file_size_bytes"),
            F.col("modificationTime").alias("modified_at"),
        )
        .withColumn("file_type", F.lit(file_type))
        .withColumn(
            "file_name",
            F.element_at(F.split(F.col("file_path"), "/"), -1),
        )
    )


def _read_archive(spark: SparkSession, path: str, **options) -> DataFrame:
    """Extract archive members, read each with its detected type, union."""
    workdir = tempfile.mkdtemp(prefix="ingest_archive_")
    local_arch = os.path.join(workdir, "archive")
    try:
        import shutil as _sh

        # materialize the archive locally (supports lake paths via hadoop fs)
        fs_path = path
        _sh.copy(fs_path, local_arch) if os.path.exists(fs_path) else None
        if not os.path.exists(local_arch):
            # lake path: pull via spark binary read -> driver bytes
            rows = (
                spark.read.format("binaryFile").load(path).select("content").collect()
            )
            if not rows:
                raise FileNotFoundError(f"archive not found: {path}")
            with open(local_arch, "wb") as fh:
                fh.write(rows[0]["content"])
        outdir = os.path.join(workdir, "members")
        os.makedirs(outdir, exist_ok=True)
        _sh.unpack_archive(local_arch, outdir)
        frames = []
        for root, _, files in os.walk(outdir):
            for fn in files:
                fp = os.path.join(root, fn)
                ft = detect_file_type(fn)
                try:
                    frames.append(read_source_file(spark, fp, ft))
                except Exception as e:  # keep going; record the failure
                    print(f"WARN: skipping archive member {fn}: {e}")
        if not frames:
            raise ValueError(f"no readable members in archive {path}")
        unioned = frames[0]
        for fr in frames[1:]:
            unioned = unioned.unionByName(fr, allowMissingColumns=True)
        return unioned.withColumn("archive_path", F.lit(path))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


APACHE_LOG_RE = (
    r'^(?P<host>\S+) \S+ (?P<user>\S+) \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<uri>\S+) \S+" (?P<status>\d{3}) (?P<bytes>\S+)'
    r'( "(?P<referrer>[^"]*)" "(?P<agent>[^"]*)")?'
)
NGINX_RE = (
    r'^(?P<host>\S+) - (?P<user>\S+) \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<uri>\S+) \S+" (?P<status>\d{3}) (?P<bytes>\d+)'
)


def _read_access_log(spark: SparkSession, path: str, flavor: str, **options) -> DataFrame:
    lines = spark.read.text(path).withColumnRenamed("value", "line")
    pattern = NGINX_RE if flavor == "nginx" else APACHE_LOG_RE
    parsed = lines.select(
        *[F.regexp_extract("line", pattern, i).alias(n)
          for i, n in enumerate(
              ["host", "user", "time", "method", "uri", "status", "bytes"], start=1)]
    )
    return parsed.withColumn("status", F.col("status").cast("int"))


def _read_w3c_log(spark: SparkSession, path: str, **options) -> DataFrame:
    rows = spark.read.text(path).withColumnRenamed("value", "line")
    fields = options.get("w3c_fields") or ["date", "time", "c-ip", "cs-method",
                                           "cs-uri-stem", "sc-status"]
    parts = F.split("line", " ")
    return rows.select(
        *[parts.getItem(i).alias(sanitize_column(f)) for i, f in enumerate(fields)]
    ).filter(~F.col("line").startswith("#"))


def _read_sqlite(spark: SparkSession, path: str, **options) -> DataFrame:
    table = options.get("dbtable")
    if not table:
        raise ValueError("sqlite needs options dbtable='<table>'")
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{path}")
        .option("dbtable", table)
        .option("driver", "org.sqlite.JDBC")
        .load()
    )


def _read_office_doc(spark: SparkSession, path: str, kind: str, **options) -> DataFrame:
    import pandas as pd

    if kind == "docx":
        from docx import Document

        doc = Document(path)
        rows = [(i, p.text) for i, p in enumerate(doc.paragraphs) if p.text.strip()]
        pdf = pd.DataFrame(rows, columns=["para_id", "text"])
    elif kind == "odt":
        from odf import text as odf_text
        from odf.opendocument import load as odf_load

        o = odf_load(path)
        paras = [str(p) for p in o.getElementsByType(odf_text.P)
                 if str(p).strip()]
        pdf = pd.DataFrame({"para_id": range(len(paras)), "text": paras})
    else:  # rtf
        from striprtf.striprtf import rtf_to_text

        with open(path, encoding="utf-8", errors="ignore") as fh:
            pdf = pd.DataFrame({"para_id": [0], "text": [rtf_to_text(fh.read())]})
    return _pandas_to_df(spark, pdf)


def _read_pdf(spark: SparkSession, path: str, **options) -> DataFrame:
    from pypdf import PdfReader
    import pandas as pd

    reader = PdfReader(path)
    rows = [(i, (pg.extract_text() or "")) for i, pg in enumerate(reader.pages)]
    return _pandas_to_df(spark, pd.DataFrame(rows, columns=["page", "text"]))


def _read_hdf5(spark: SparkSession, path: str, **options) -> DataFrame:
    import h5py
    import pandas as pd
    import numpy as np

    ds = options.get("dataset")  # e.g. "/measurements/values"
    with h5py.File(path, "r") as h:
        key = ds or list(h.keys())[0]

        def _get(k):
            o = h[k]
            return o[()] if not isinstance(o, h5py.Group) else None

        data = _get(key)
    arr = np.asarray(data)
    pdf = pd.DataFrame(arr.reshape(arr.shape[0], -1)) if arr.ndim > 1 else pd.DataFrame({"value": arr.ravel()})
    return _pandas_to_df(spark, pdf).withColumn("hdf5_dataset", F.lit(key))


def _read_netcdf(spark: SparkSession, path: str, **options) -> DataFrame:
    import xarray as xr

    ds = xr.open_dataset(path)
    pdf = ds.to_dataframe().reset_index()
    ds.close()
    return _pandas_to_df(spark, pdf)


def _read_numpy(spark: SparkSession, path: str, **options) -> DataFrame:
    import numpy as np
    import pandas as pd

    obj = np.load(path, allow_pickle=True)
    arr = obj[list(obj.keys())[0]] if isinstance(obj, np.lib.npyio.NpzFile) else obj
    arr = np.asarray(arr)
    pdf = (pd.DataFrame(arr.reshape(arr.shape[0], -1))
           if arr.ndim > 1 else pd.DataFrame({"value": arr.ravel()}))
    return _pandas_to_df(spark, pdf)


def _read_pickle_df(spark: SparkSession, path: str, **options) -> DataFrame:
    import pandas as pd

    obj = pd.read_pickle(path)
    pdf = obj if isinstance(obj, pd.DataFrame) else pd.DataFrame({"value": obj})
    return _pandas_to_df(spark, pdf)


def _read_geodataframe(spark: SparkSession, path: str, **options) -> DataFrame:
    import geopandas as gpd

    gdf = gpd.read_file(path)
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    return _pandas_to_df(spark, gdf.drop(columns=["geometry"]))


def _read_edi(spark: SparkSession, path: str, **options) -> DataFrame:
    seg_term = options.get("segment_terminator", "'")
    elem_sep = options.get("element_separator", "+")
    lines = spark.read.text(path).withColumnRenamed("value", "raw")
    content = "".join(r["raw"] for r in lines.collect())
    segs = [s for s in content.replace("\n", "").split(seg_term) if s.strip()]
    rows = [(s.split(elem_sep)[0], elem_sep.join(s.split(elem_sep)[1:]))
            for s in segs]
    return spark.createDataFrame(rows, ["segment", "elements"])


def _read_config_kv(spark: SparkSession, path: str, **options) -> DataFrame:
    """Config files -> (section, key, value) rows."""
    import configparser

    text = "\n".join(r["value"] for r in spark.read.text(path).collect())
    rows: List[Tuple[str, str, str]] = []
    try:
        cp = configparser.ConfigParser()
        cp.read_string(text)
        for sec in cp.sections():
            for k, v in cp.items(sec):
                rows.append((sec, k, v))
    except Exception:
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith(("#", ";", "[")):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                rows.append(("default", k.strip(), v.strip()))
    return spark.createDataFrame(rows, ["section", "key", "value"])


def _read_serialized(spark: SparkSession, path: str, lib: str, **options) -> DataFrame:
    """msgpack / cbor / bson -> pandas -> DataFrame."""
    import pandas as pd

    raw = spark.sparkContext.binaryFiles(path).values().collect()
    if not raw:
        raise ValueError(f"empty file: {path}")
    blob = bytes(raw[0])
    if lib == "msgpack":
        import msgpack

        obj = msgpack.unpackb(blob, raw=False)
    elif lib == "cbor":
        import cbor2

        obj = cbor2.loads(blob)
    else:
        import bson

        obj = bson.decode_all(blob)
    if isinstance(obj, dict):
        obj = [obj]
    return _pandas_to_df(spark, pd.json_normalize(obj))


def _read_libsvm(spark: SparkSession, path: str, **options) -> DataFrame:
    return spark.read.format("libsvm").load(path)


def _read_arff(spark: SparkSession, path: str, **options) -> DataFrame:
    from scipy.io import arff
    import pandas as pd

    data, _ = arff.loadarff(path)
    return _pandas_to_df(spark, pd.DataFrame(data))


def _read_sequence_file(spark: SparkSession, path: str, **options) -> DataFrame:
    rdd = spark.sparkContext.sequenceFile(path)
    return rdd.toDF(["key", "value"])


def _read_tfrecord(spark: SparkSession, path: str, **options) -> DataFrame:
    try:
        return spark.read.format("tfrecords").load(
            path, schema=options.get("tf_schema")
        )
    except Exception as e:
        raise RuntimeError(
            "TFRecord needs the spark-tfrecord package "
            "(--packages com.linkedin.sparktfrecord:spark-tfrecord_2.12:<ver>)"
        ) from e


def _read_lance(spark: SparkSession, path: str, **options) -> DataFrame:
    import lance

    ds = lance.dataset(path)
    return _pandas_to_df(spark, ds.to_table().to_pandas())


def _read_stats_file(spark: SparkSession, path: str, kind: str, **options) -> DataFrame:
    import pandas as pd

    if kind == "sas":
        pdf = pd.read_sas(path)
    elif kind == "spss":
        import pyreadstat

        pdf, _ = pyreadstat.read_sav(path)
    elif kind == "stata":
        pdf = pd.read_stata(path)
    elif kind == "rdata":
        import pyreadr

        res = pyreadr.read_r(path)
        pdf = res[list(res.keys())[0]]
    elif kind == "mat":
        from scipy.io import loadmat
        import numpy as np

        d = loadmat(path)
        cols = {k: np.asarray(v).ravel() for k, v in d.items()
                if not k.startswith("__")}
        pdf = pd.DataFrame({k: v for k, v in cols.items()
                            if v.ndim == 1})
    elif kind == "fits":
        from astropy.io import fits

        with fits.open(path) as h:
            data = h[1].data if len(h) > 1 else h[0].data
        pdf = pd.DataFrame(data.tolist())
    else:
        raise ValueError(f"unknown stats kind {kind}")
    return _pandas_to_df(spark, pdf)


def _read_evtx(spark: SparkSession, path: str, **options) -> DataFrame:
    try:
        from Evtx.Evtx import Evtx

        rows = [(r.xml()) for r in Evtx(path).records()]
        return spark.createDataFrame([(x,) for x in rows], ["event_xml"])
    except ImportError:
        return _read_binary_meta(spark, path, "evtx").withColumn(
            "note", F.lit("install python-evtx for parsed rows")
        )


# ====================== CENTRAL DISPATCH ======================
def read_source_file(
    spark: SparkSession, path: str, file_type: str, **options
) -> DataFrame:
    """Read any supported source file into a DataFrame.

    Dispatches on the normalized file_type; raises ValueError listing the
    supported types when the type is unknown.
    """
    ft = normalize_file_type(file_type)

    # ---- structured / delimited ----
    if ft in ("csv", "psv"):
        return _read_delimited(spark, path, "," if ft == "csv" else "|", **options)
    if ft == "tsv":
        return _read_delimited(spark, path, "\t", **options)
    if ft in ("txt", "tab", "dat", "bcp"):
        sep = options.get("sep", "\t" if ft in ("tsv", "tab") else ",")
        if options.get("as_text"):
            return _read_text_lines(spark, path, **options)
        return _read_delimited(spark, path, sep, **options)
    if ft in ("scsv", "semicolon"):
        return _read_delimited(spark, path, ";", **options)
    if ft == "fixed":
        return _read_fixed_width(spark, path, **options)
    if ft in ("xlsx", "xls", "xlsm"):
        return _read_excel(spark, path, **options)
    if ft == "ods":
        import pandas as pd

        return _pandas_to_df(spark, pd.read_excel(path, engine="odf"))
    if ft in ("dif", "slk"):
        return _read_text_lines(spark, path, **options)

    # ---- semi-structured ----
    if ft in ("json", "jsonl", "geojson"):
        return _read_json(spark, path, **options)
    if ft in ("json5", "jsonc"):
        return _read_json5(spark, path, **options)
    if ft in ("xml", "rss", "atom", "kml", "gpx", "gml"):
        return _read_xml(spark, path, **options)
    if ft == "yaml":
        return _read_yaml(spark, path, **options)
    if ft == "html":
        return _read_text_lines(spark, path, **options)
    if ft == "avro":
        return spark.read.format("avro").load(path)

    # ---- big data / analytics ----
    if ft == "parquet":
        return spark.read.parquet(path)
    if ft == "orc":
        return spark.read.orc(path)
    if ft in ("arrow", "feather", "ipc"):
        import pandas as pd
        import pyarrow.feather as pf

        local = _materialize_local(spark, path)
        return _pandas_to_df(spark, pd.DataFrame(pf.read_table(local).to_pandas()))
    if ft == "delta":
        return spark.read.format("delta").load(path)
    if ft == "hudi":
        return spark.read.format("hudi").load(path)
    if ft == "iceberg":
        return spark.read.format("iceberg").load(path)
    if ft == "seq":
        return _read_sequence_file(spark, path, **options)
    if ft == "tfrecord":
        return _read_tfrecord(spark, path, **options)
    if ft == "lance":
        return _read_lance(spark, path, **options)
    if ft == "rc":
        return _read_binary_meta(spark, path, ft).withColumn(
            "note", F.lit("RCFile is Hive-native; use Hive serde or convert first")
        )

    # ---- database exports ----
    if ft == "sql":
        return _read_text_lines(spark, path, **options).withColumnRenamed(
            "line", "statement"
        )
    if ft == "sqlite":
        return _read_sqlite(spark, path, **options)
    if ft in ("mdb", "accdb", "dmp", "bak"):
        return _read_binary_meta(spark, path, ft)

    # ---- logs ----
    if ft == "log":
        flavor = options.get("log_format", "raw")
        if flavor in ("apache", "apache_common", "apache_combined"):
            return _read_access_log(spark, path, "apache", **options)
        if flavor == "nginx":
            return _read_access_log(spark, path, "nginx", **options)
        if flavor in ("w3c", "elf"):
            return _read_w3c_log(spark, path, **options)
        if flavor == "json":
            return _read_json(spark, path, **options)
        if flavor == "csv":
            return _read_delimited(spark, path, ",", **options)
        return _read_text_lines(spark, path, **options)
    if ft == "evtx":
        return _read_evtx(spark, path, **options)

    # ---- compressed / archives ----
    if ft in ("gz", "bz2", "xz", "zst", "lz4", "snappy", "deflate"):
        inner = options.get("inner_file_type", "txt")
        if inner in ("txt", "log", "csv", "json", "jsonl"):
            # Hadoop codecs handle splittable compression transparently
            return read_source_file(spark, path, inner, **options)
        return _read_binary_meta(spark, path, ft)
    if ft in ("zip", "tar", "tar.gz", "tar.bz2", "tar.xz", "7z", "rar"):
        return _read_archive(spark, path, **options)

    # ---- documents ----
    if ft in ("md", "tex", "ps"):
        return _read_text_lines(spark, path, **options)
    if ft == "rtf":
        return _read_office_doc(spark, path, "rtf", **options)
    if ft == "docx":
        return _read_office_doc(spark, path, "docx", **options)
    if ft == "doc":
        return _read_binary_meta(spark, path, ft).withColumn(
            "note", F.lit("legacy .doc needs antiword/libreoffice conversion")
        )
    if ft == "odt":
        return _read_office_doc(spark, path, "odt", **options)
    if ft == "pdf":
        return _read_pdf(spark, path, **options)
    if ft == "epub":
        return _read_text_lines(spark, path, **options)

    # ---- media (file-catalog pattern) ----
    if ft in ("jpg", "png", "gif", "tiff", "bmp", "webp", "heic",
              "heif", "svg", "jp2", "avif", "dcm", "raw",
              "wav", "mp3", "aac", "flac", "ogg", "opus", "aiff",
              "m4a", "wma", "amr", "pcm",
              "mp4", "mpeg", "mov", "avi", "mkv", "webm", "wmv",
              "flv", "m4v", "mts", "m2ts", "3gp"):
        return _read_binary_meta(spark, path, ft, **options)

    # ---- geospatial ----
    if ft in ("shp", "gpkg", "fgb"):
        return _read_geodataframe(spark, path, **options)
    if ft in ("kmz",):
        return _read_binary_meta(spark, path, ft)
    if ft in ("tiff", "asc", "las", "laz", "grib"):
        # GeoTIFF shares the tiff extension: route by explicit file_type
        if ft == "tiff" and options.get("is_geotiff"):
            return _read_binary_meta(spark, path, "geotiff")
        return _read_binary_meta(spark, path, ft)

    # ---- scientific ----
    if ft in ("h5", "hdf5", "h4"):
        return _read_hdf5(spark, path, **options)
    if ft == "nc":
        return _read_netcdf(spark, path, **options)
    if ft in ("npy", "npz"):
        return _read_numpy(spark, path, **options)
    if ft in ("pkl", "pickle", "joblib"):
        return _read_pickle_df(spark, path, **options)
    if ft in ("rda", "rdata", "rds"):
        return _read_stats_file(spark, path, "rdata", **options)
    if ft == "sas7bdat":
        return _read_stats_file(spark, path, "sas", **options)
    if ft == "sav":
        return _read_stats_file(spark, path, "spss", **options)
    if ft == "dta":
        return _read_stats_file(spark, path, "stata", **options)
    if ft == "mat":
        return _read_stats_file(spark, path, "mat", **options)
    if ft in ("fits", "fit"):
        return _read_stats_file(spark, path, "fits", **options)
    if ft == "nb":
        return _read_json(spark, path, **options)

    # ---- config / metadata ----
    if ft in ("toml",):
        import tomllib

        raw = b"".join(
            spark.sparkContext.binaryFiles(path).values().collect()
        )
        import pandas as pd

        flat = pd.json_normalize(tomllib.loads(raw.decode()))
        return _pandas_to_df(spark, flat)
    if ft in ("ini", "properties", "env"):
        return _read_config_kv(spark, path, **options)
    if ft in ("proto", "avsc", "tf", "tfvars", "hcl"):
        return _read_text_lines(spark, path, **options)

    # ---- api / exchange binaries ----
    if ft in ("msgpack", "cbor", "bson"):
        return _read_serialized(spark, path, ft, **options)
    if ft in ("pb", "proto"):
        return _read_binary_meta(spark, path, ft).withColumn(
            "note", F.lit("protobuf needs the compiled schema; decode after codegen")
        )
    if ft == "thrift":
        return _read_binary_meta(spark, path, ft)

    # ---- ml / ai ----
    if ft in ("svm", "libsvm"):
        return _read_libsvm(spark, path, **options)
    if ft == "arff":
        return _read_arff(spark, path, **options)
    if ft == "onnx":
        return _read_binary_meta(spark, path, ft)

    # ---- enterprise text-ish ----
    if ft in ("edi", "edifact", "x12", "idoc"):
        return _read_edi(spark, path, **options)

    raise ValueError(
        f"unsupported file_type '{file_type}'. "
        f"Detected '{ft}'. Pass a supported type or add a reader."
    )


def _materialize_local(spark: SparkSession, path: str) -> str:
    """Pull a (small) lake file to a local temp path for pandas readers."""
    if os.path.exists(path):
        return path
    rows = spark.read.format("binaryFile").load(path).select("content").collect()
    if not rows:
        raise FileNotFoundError(path)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.basename(path))
    tmp.write(bytes(rows[0]["content"]))
    tmp.close()
    return tmp.name


# ====================== HIVE WRITE + CORE ======================
def write_hive_table(
    spark: SparkSession,
    df: DataFrame,
    hive_table: str,
    partition_by: Optional[List[str]] = None,
    mode: str = "append",
) -> str:
    """Write a DataFrame to a Hive table backed by the data lake."""
    db, table = parse_table_name(hive_table)
    table = sanitize_column(table)
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {db} "
        f"LOCATION '{CONFIG['lake_base_path']}/{db}.db'"
    )
    location = f"{CONFIG['lake_base_path']}/{db}.db/{table}"
    writer = (
        df.write.mode(mode)
        .format("parquet")
        .option("mergeSchema", "true")
        .option("path", location)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(f"{db}.{table}")
    return f"{db}.{table}"


def _audit(
    spark: SparkSession,
    category: str,
    source_path: str,
    file_type: str,
    hive_table: str,
    row_count: int,
    status: str = "SUCCESS",
    message: str = "",
) -> None:
    """Operation: data auditing - one row per ingestion run."""
    try:
        db = CONFIG["audit_table"].split(".")[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        audit = spark.createDataFrame(
            [(category, source_path, file_type, hive_table, row_count,
              status, message, F.current_timestamp())],
            ["category", "source_path", "file_type", "hive_table",
             "row_count", "status", "message", "run_at"],
        )
        audit.write.mode("append").format("delta").saveAsTable(CONFIG["audit_table"])
    except Exception as e:
        print(f"WARN: audit write failed: {e}")


def _lineage(
    spark: SparkSession, source_path: str, hive_table: str, transform: str
) -> None:
    """Operation: data lineage - source -> target edge."""
    try:
        db = CONFIG["lineage_table"].split(".")[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        edge = spark.createDataFrame(
            [(source_path, hive_table, transform, F.current_timestamp())],
            ["source", "target", "transform", "run_at"],
        )
        edge.write.mode("append").format("delta").saveAsTable(CONFIG["lineage_table"])
    except Exception as e:
        print(f"WARN: lineage write failed: {e}")


def _ingest(
    spark: SparkSession,
    category: str,
    allowed_types: set,
    source_path: str,
    file_type: str,
    hive_table: str,
    **options,
) -> str:
    """Shared core: validate type -> read -> optional ops -> Hive."""
    ft = normalize_file_type(file_type)
    if ft not in allowed_types:
        raise ValueError(
            f"[{category}] file_type '{file_type}' not in this category. "
            f"Allowed: {sorted(allowed_types)}"
        )
    try:
        df = read_source_file(spark, source_path, ft, **options)
        if options.get("dedupe"):
            df = df.dropDuplicates(options.get("dedupe_columns"))
        if options.get("filter_expr"):
            df = df.filter(options["filter_expr"])
        target = write_hive_table(
            spark, df,
            hive_table,
            partition_by=options.get("partition_by"),
            mode=options.get("mode", "append"),
        )
        try:
            row_count = df.count()
        except Exception:
            row_count = -1
        _audit(spark, category, source_path, ft, target, row_count)
        _lineage(spark, source_path, target, f"ingest:{category}:{ft}")
        return target
    except Exception as e:
        _audit(spark, category, source_path, ft, hive_table, -1,
               status="FAILED", message=str(e)[:500])
        raise


# ================== CATEGORY FUNCTIONS (1-19) ==================
# Each takes (spark, source_path, file_type, hive_table, **options).

def ingest_structured_data_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 1: structured files -> Hive. file_type: csv/tsv/txt/psv/
    scsv/fixed/xlsx/xls/xlsm/ods/dif/slk. Fixed-width needs
    options widths=[...], columns=[...]."""
    return _ingest(spark, "structured", {
        "csv", "tsv", "txt", "psv", "scsv", "semicolon", "fixed",
        "xlsx", "xls", "xlsm", "ods", "dif", "slk", "tab", "dat"},
        source_path, file_type, hive_table, **options)


def ingest_semi_structured_data_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 2: semi-structured -> Hive. file_type: json/jsonl/xml/yaml/
    html/rss/atom/geojson/json5/jsonc/avro. XML uses options rowTag."""
    return _ingest(spark, "semi_structured", {
        "json", "jsonl", "xml", "yaml", "html", "rss", "atom",
        "geojson", "json5", "jsonc", "avro"},
        source_path, file_type, hive_table, **options)


def ingest_bigdata_analytics_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 3: big-data formats -> Hive. file_type: parquet/orc/avro/
    arrow/delta/hudi/iceberg/seq/tfrecord/lance/rc."""
    return _ingest(spark, "bigdata", {
        "parquet", "orc", "avro", "arrow", "feather", "ipc",
        "delta", "hudi", "iceberg", "seq", "tfrecord", "lance", "rc"},
        source_path, file_type, hive_table, **options)


def ingest_database_export_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 4: DB exports -> Hive. file_type: sql/sqlite/csv/json/xml/
    bcp/dat/tab. sqlite needs options dbtable='<table>'."""
    return _ingest(spark, "db_export", {
        "sql", "sqlite", "csv", "json", "xml", "bcp", "dat", "tab",
        "mdb", "accdb", "dmp", "bak"},
        source_path, file_type, hive_table, **options)


def ingest_log_event_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 5: logs/events -> Hive. file_type: log/evtx/json/jsonl/csv.
    options log_format: raw|apache|nginx|w3c|elf|json|csv."""
    return _ingest(spark, "logs", {"log", "evtx", "json", "jsonl", "csv"},
                   source_path, file_type, hive_table, **options)


def ingest_compressed_archived_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 6: compressed/archives -> Hive. file_type: zip/gz/tar/
    tar.gz/bz2/xz/7z/rar/zst/lz4/snappy/deflate. Single-file codecs stream
    through; archives are extracted and members read by detected type."""
    return _ingest(spark, "compressed", {
        "zip", "gz", "tar", "tar.gz", "bz2", "tar.bz2", "xz", "tar.xz",
        "7z", "rar", "zst", "lz4", "snappy", "deflate", "tgz", "tbz2"},
        source_path, file_type, hive_table, **options)


def ingest_text_document_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 7: documents -> Hive (page/paragraph grain). file_type:
    txt/rtf/doc/docx/odt/pdf/md/tex/ps/epub."""
    return _ingest(spark, "documents", {
        "txt", "rtf", "doc", "docx", "odt", "pdf", "md", "tex",
        "ps", "epub"},
        source_path, file_type, hive_table, **options)


def ingest_image_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 8: images -> Hive FILE CATALOG table. file_type: jpg/png/
    gif/tiff/bmp/webp/heic/heif/svg/jp2/avif/dcm/raw."""
    return _ingest(spark, "images", {
        "jpg", "png", "gif", "tiff", "bmp", "webp", "heic", "heif",
        "svg", "jp2", "avif", "dcm", "raw"},
        source_path, file_type, hive_table, **options)


def ingest_audio_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 9: audio -> Hive FILE CATALOG table. file_type: wav/mp3/
    aac/flac/ogg/opus/aiff/m4a/wma/amr/pcm."""
    return _ingest(spark, "audio", {
        "wav", "mp3", "aac", "flac", "ogg", "opus", "aiff",
        "m4a", "wma", "amr", "pcm"},
        source_path, file_type, hive_table, **options)


def ingest_video_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 10: video -> Hive FILE CATALOG table. file_type: mp4/mpeg/
    mov/avi/mkv/webm/wmv/flv/m4v/mts/m2ts/3gp."""
    return _ingest(spark, "video", {
        "mp4", "mpeg", "mov", "avi", "mkv", "webm", "wmv",
        "flv", "m4v", "mts", "m2ts", "3gp"},
        source_path, file_type, hive_table, **options)


def ingest_geospatial_gis_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 11: geospatial -> Hive. file_type: shp/geojson/kml/kmz/
    gpkg/gml/tiff(geotiff via options is_geotiff)/asc/nc/h5/grib/fgb/
    las/laz/gpx. Vector formats flatten to WKT geometry."""
    return _ingest(spark, "geospatial", {
        "shp", "geojson", "kml", "kmz", "gpkg", "gml", "tiff",
        "asc", "nc", "h5", "grib", "fgb", "las", "laz", "gpx", "xml"},
        source_path, file_type, hive_table, **options)


def ingest_scientific_engineering_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 12: scientific -> Hive. file_type: h5/nc/mat/fits/grib/
    npy/npz/pkl/rda/rds/sas7bdat/sav/dta/nb. hdf5 uses options dataset."""
    return _ingest(spark, "scientific", {
        "h5", "hdf5", "h4", "nc", "mat", "fits", "fit", "grib",
        "npy", "npz", "pkl", "pickle", "rda", "rdata", "rds",
        "sas7bdat", "sav", "dta", "nb"},
        source_path, file_type, hive_table, **options)


def ingest_cloud_datalake_objects(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 13: cloud objects (s3a://, abfss://, gs://) -> Hive.
    file_type auto-detected from the object key when 'auto'."""
    ft = detect_file_type(source_path) if file_type == "auto" else file_type
    return _ingest(spark, "cloud_objects", {
        "csv", "tsv", "txt", "json", "jsonl", "parquet", "orc",
        "avro", "xml", "zip", "gz", "tar.gz", "delta", "log"},
        source_path, ft, hive_table, **options)


def ingest_config_metadata_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 14: config/metadata -> Hive key-value tables. file_type:
    json/yaml/xml/toml/ini/properties/env/md/proto/avsc/tf/hcl/sql."""
    df_table = _ingest(spark, "config", {
        "json", "yaml", "xml", "toml", "ini", "properties", "env",
        "txt", "md", "proto", "avsc", "tf", "tfvars", "hcl", "sql"},
        source_path, file_type, hive_table, **options)
    return df_table


def ingest_api_exchange_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 15: API/exchange payloads -> Hive. file_type: json/jsonl/
    xml/csv/proto/pb/avro/msgpack/cbor/bson/ndjson."""
    return _ingest(spark, "api_exchange", {
        "json", "jsonl", "xml", "csv", "proto", "pb", "avro",
        "msgpack", "cbor", "bson", "ndjson"},
        source_path, file_type, hive_table, **options)


def ingest_streaming_messaging_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 16: streaming message dumps -> Hive. file_type: json/avro/
    proto/thrift/msgpack/cbor/bson/csv/txt(text msgs)/xml."""
    return _ingest(spark, "streaming", {
        "json", "avro", "proto", "pb", "thrift", "msgpack", "cbor",
        "bson", "csv", "txt", "xml"},
        source_path, file_type, hive_table, **options)


def ingest_ml_ai_data_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 17: ML/AI datasets -> Hive. file_type: csv/json/jsonl/
    parquet/arrow/npy/npz/tfrecord/h5/svm/libsvm/arff/onnx/pkl/joblib/xml."""
    return _ingest(spark, "ml_ai", {
        "csv", "json", "jsonl", "parquet", "arrow", "feather", "npy",
        "npz", "tfrecord", "h5", "hdf5", "svm", "libsvm", "arff",
        "onnx", "pkl", "pickle", "joblib", "xml", "txt"},
        source_path, file_type, hive_table, **options)


def ingest_enterprise_export_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 18: enterprise exports -> Hive. file_type: csv/xlsx/xls/
    json/xml/txt/edi/edifact/x12/idoc. EDI uses options segment_terminator,
    element_separator."""
    return _ingest(spark, "enterprise", {
        "csv", "xlsx", "xls", "json", "xml", "txt", "edi",
        "edifact", "x12", "idoc"},
        source_path, file_type, hive_table, **options)


def ingest_recommended_source_files(
    spark: SparkSession, source_path: str, file_type: str,
    hive_table: str, **options
) -> str:
    """Category 19: product-recommended subset -> Hive. The curated list the
    data product officially supports (csv..libsvm, 36 types)."""
    return _ingest(spark, "recommended", {
        "csv", "tsv", "txt", "xlsx", "xls", "json", "jsonl", "xml",
        "yaml", "parquet", "orc", "avro", "arrow", "zip", "gz",
        "tar.gz", "sql", "sqlite", "log", "pdf", "docx", "md",
        "geojson", "shp", "gpkg", "h5", "nc", "jpg", "png", "wav",
        "mp3", "mp4", "tfrecord", "npy", "npz", "arff", "svm", "libsvm"},
        source_path, file_type, hive_table, **options)


# ================== CATEGORY 20: OPERATIONS ==================
def data_engineering_operations(
    spark: SparkSession, target, operation: str, **options
):
    """Category 20: data engineering operations.

    target: a DataFrame, a Hive table name, or a source path (per operation).
    operation: one of the 21 supported operations below.
    """
    op = operation.strip().lower()

    if op == "file_ingestion":
        # target=(category_fn, source_path, file_type, hive_table)
        fn, sp, ft, ht = target
        return fn(spark, sp, ft, ht, **options)

    if op == "file_format_detection":
        return detect_file_type(target)

    if op == "schema_inference":
        ft = options.get("file_type") or detect_file_type(target)
        df = read_source_file(spark, target, ft, **options)
        return df.schema.json()

    if op == "data_validation":
        df = target if isinstance(target, DataFrame) else spark.table(target)
        rules = options.get("rules", {})  # {col: {"not_null": True, "min": x}}
        violations = []
        for col, rule in rules.items():
            if rule.get("not_null"):
                violations.append(
                    df.filter(F.col(col).isNull()).limit(5)
                    .withColumn("violation", F.lit(f"{col} null"))
                )
        return violations[0] if violations else spark.createDataFrame(
            [], StructType([StructField("violation", StringType())]))

    if op == "data_transformation":
        df = target if isinstance(target, DataFrame) else spark.table(target)
        if options.get("select_exprs"):
            df = df.selectExpr(*options["select_exprs"])
        return df

    if op == "file_format_conversion":
        # target=(source_path, file_type); options: dest_path, dest_format
        sp, ft = target
        df = read_source_file(spark, sp, ft, **options)
        df.write.mode("overwrite").format(
            options.get("dest_format", "parquet")
        ).save(options["dest_path"])
        return options["dest_path"]

    if op == "incremental_ingestion":
        # options: manifest_table, path_col='source_path'
        manifest = options["manifest_table"]
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {manifest} "
            "(source_path STRING, processed_at TIMESTAMP) USING DELTA"
        )
        done = {r[0] for r in spark.table(manifest).select("source_path").collect()}
        new_files = [f for f in options["candidate_files"] if f not in done]
        for f in new_files:
            options["category_fn"](spark, f, detect_file_type(f),
                                   options["hive_table"])
            spark.sql(f"INSERT INTO {manifest} VALUES ('{f}', current_timestamp())")
        return new_files

    if op == "data_quality_checks":
        df = target if isinstance(target, DataFrame) else spark.table(target)
        n = df.count()
        rows = []
        for c in df.columns:
            nulls = df.filter(F.col(f"`{c}`").isNull()).count()
            distinct = df.select(f"`{c}`").distinct().count()
            rows.append((c, n, nulls, round(nulls / n, 4) if n else 0.0, distinct))
        return spark.createDataFrame(
            rows, ["column", "row_count", "null_count", "null_rate", "distinct_count"])

    if op == "metadata_extraction":
        ft = options.get("file_type") or detect_file_type(target)
        return _read_binary_meta(spark, target, ft)

    if op == "data_partitioning":
        df = target if isinstance(target, DataFrame) else spark.table(target)
        return write_hive_table(spark, df, options["hive_table"],
                                partition_by=options.get("partition_by"))

    if op == "record_count_validation":
        src, src_ft = target  # (source_path, file_type)
        src_df = read_source_file(spark, src, src_ft)
        tgt_n = spark.table(options["hive_table"]).count()
        return {"source_count": src_df.count(), "target_count": tgt_n,
                "match": src_df.count() == tgt_n}

    if op == "duplicate_detection":
        df = target if isinstance(target, DataFrame) else spark.table(target)
        keys = options.get("keys") or df.columns
        return (df.groupBy(*keys).count()
                .filter("count > 1").orderBy(F.desc("count")))

    if op == "null_value_detection":
        df = target if isinstance(target, DataFrame) else spark.table(target)
        n = df.count()
        exprs = [F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c)
                 for c in df.columns]
        counts = df.select(*exprs).collect()[0].asDict()
        return {c: {"nulls": counts[c],
                    "rate": round(counts[c] / n, 4) if n else 0.0}
                for c in df.columns}

    if op == "malformed_file_detection":
        ft = options.get("file_type") or detect_file_type(target)
        df = (spark.read.option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt")
              .csv(target) if ft == "csv"
              else read_source_file(spark, target, ft, **options))
        if "_corrupt" in df.columns:
            return df.filter(F.col("_corrupt").isNotNull())
        return df.limit(0)

    if op == "schema_evolution":
        # compare source schema vs Hive table; ADD new columns
        sp, ft = target
        new_schema = read_source_file(spark, sp, ft, **options).schema
        db, table = parse_table_name(options["hive_table"])
        cur = {f.name.lower() for f in spark.table(f"{db}.{table}").schema.fields}
        adds = [f for f in new_schema.fields if f.name.lower() not in cur]
        for f in adds:
            spark.sql(f"ALTER TABLE {db}.{table} ADD COLUMN "
                      f"{sanitize_column(f.name)} {f.dataType.simpleString()}")
        return [f.name for f in adds]

    if op == "security_scanning":
        # allowlist + size guard scan (wire real AV/DLP at this hook)
        allow = set(options.get("allowed_types", []))
        max_mb = options.get("max_mb", 1024)
        meta = _read_binary_meta(spark, target, detect_file_type(target))
        flagged = meta.filter(
            (F.col("file_size_bytes") > max_mb * 1024 * 1024)
            | (~F.col("file_type").isin(list(allow)) if allow else F.lit(False))
        )
        return flagged

    if op == "error_handling":
        fn, args = target  # (callable, arg-tuple); returns (result, error)
        try:
            return fn(*args), None
        except Exception as e:
            return None, str(e)[:1000]

    if op in ("data_auditing", "data_lineage", "pipeline_monitoring"):
        # audit/lineage are written automatically by _ingest; monitoring reads them
        if op == "pipeline_monitoring":
            return spark.table(CONFIG["audit_table"]).orderBy(F.desc("run_at"))
        return f"operation '{op}' is applied automatically during ingestion"

    raise ValueError(
        f"unknown operation '{operation}'. Supported: file_ingestion, "
        "file_format_detection, schema_inference, data_validation, "
        "data_transformation, file_format_conversion, incremental_ingestion, "
        "data_quality_checks, metadata_extraction, data_partitioning, "
        "record_count_validation, duplicate_detection, null_value_detection, "
        "malformed_file_detection, schema_evolution, security_scanning, "
        "error_handling, data_auditing, data_lineage, pipeline_monitoring."
    )


# ============================ main ============================
def main() -> None:
    """Demo: ingest one file per representative category + run ops."""
    spark = get_spark()
    landing = CONFIG["landing_zone"]
    jobs = [
        (ingest_structured_data_files, f"{landing}/orders.csv", "csv", "lake.orders"),
        (ingest_semi_structured_data_files, f"{landing}/events.jsonl", "jsonl", "lake.events"),
        (ingest_bigdata_analytics_files, f"{landing}/sales.parquet", "parquet", "lake.sales"),
        (ingest_log_event_files, f"{landing}/access.log", "log", "lake.access_logs"),
        (ingest_compressed_archived_files, f"{landing}/dump.zip", "zip", "lake.dump"),
        (ingest_text_document_files, f"{landing}/docs/report.pdf", "pdf", "lake.documents"),
        (ingest_image_files, f"{landing}/images/", "jpg", "lake.image_catalog"),
        (ingest_geospatial_gis_files, f"{landing}/zones.geojson", "geojson", "lake.zones"),
        (ingest_ml_ai_data_files, f"{landing}/train.libsvm", "libsvm", "lake.train"),
        (ingest_enterprise_export_files, f"{landing}/orders.edi", "edi", "lake.edi_orders"),
    ]
    for fn, path, ft, table in jobs:
        try:
            target = fn(spark, path, ft, table)
            print(f"OK  {fn.__name__}({ft}) -> {target}")
        except Exception as e:
            print(f"SKIP {fn.__name__}({path}): {e}")

    dq = data_engineering_operations(spark, "lake.orders", "data_quality_checks")
    dq.show(truncate=False)


if __name__ == "__main__":
    main()
