"""
Neo4j neo4j-admin import 형식의 CSV를 빌드하고 S3에 업로드하기 위한 공용 유틸.

이 모듈은 catalog/도메인에 의존하지 않는다.
- 컬럼 정의:        CsvColumn, RelationshipSpec
- CSV 직렬화:       build_node_csv, build_relationship_csv
- S3 업로드:        upload_csv_to_s3

S3 path 산정은 oliveyoung_common.s3_paths.neo4j_csv_prefix() 를 사용한다.

호출 측 책임:
- :ID 컬럼은 unique 해야 한다. 호출 측이 미리 dedup 한다.
- 빈 DataFrame은 caller가 skip 결정. 이 모듈은 빈 DataFrame을 받으면 헤더 단독
  CSV가 생성되므로, caller는 비었을 때 이 모듈을 호출하지 않는 것이 권장된다.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

import boto3
import pandas as pd


# ==========================================
# 컬럼/관계 정의
# ==========================================

@dataclass(frozen=True)
class CsvColumn:
    """Neo4j neo4j-admin import 형식의 단일 컬럼 정의.

    Neo4j 헤더는 다음 3축의 조합으로 결정된다:
        (1) source : DataFrame 원본 컬럼명. None 이면 name 을 사용.
        (2) name   : 출력 property 이름. ":LABEL" 같은 메타 컬럼은 빈 문자열.
        (3) tag    : Neo4j 타입 태그. ":ID(Label)", ":int", ":string[]", ":LABEL" 등.

    Examples:
        CsvColumn("product_id", is_id=True, id_space="Product")
            → 헤더: "product_id:ID(Product)"
        CsvColumn("brand", source="product_brand")
            → 헤더: "brand"
        CsvColumn("tags", type_hint="string[]")
            → 헤더: "tags:string[]", 값은 array_delim(`;`)으로 join
        CsvColumn("", is_label=True, source="labels")
            → 헤더: ":LABEL", 값(list)은 ";" 로 join
    """
    name: str
    source: Optional[str] = None
    type_hint: Optional[str] = None
    is_id: bool = False
    id_space: Optional[str] = None
    is_label: bool = False
    array_delim: str = ";"
    transform: Optional[Callable[[Any], Any]] = None

    def header(self) -> str:
        if self.is_id:
            space = self.id_space or ""
            return f"{self.name}:ID({space})" if space else f"{self.name}:ID"
        if self.is_label:
            return ":LABEL"
        if self.type_hint:
            return f"{self.name}:{self.type_hint}"
        return self.name

    def resolve_source(self) -> str:
        return self.source if self.source is not None else self.name


@dataclass(frozen=True)
class RelationshipSpec:
    """관계 CSV 정의.

    헤더는 자동으로 ":START_ID(<start_label>),:END_ID(<end_label>),:TYPE,...properties" 형태가 된다.
    properties 의 각 CsvColumn 은 노드에서 쓴 것과 동일하게 type_hint / transform 을 사용한다.
    """
    start_label: str
    end_label: str
    rel_type: str
    start_col: str
    end_col: str
    properties: list[CsvColumn] = field(default_factory=list)


# ==========================================
# 내부 helper
# ==========================================

def _format_value(value: Any, col: CsvColumn) -> str:
    """단일 셀 값을 CSV에 들어갈 문자열로 정규화."""
    if col.transform is not None:
        value = col.transform(value)

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""

    # array 타입 또는 :LABEL: list 를 array_delim 으로 join
    is_array_type = col.is_label or (col.type_hint or "").endswith("[]")
    if is_array_type and isinstance(value, (list, tuple)):
        return col.array_delim.join("" if v is None else str(v) for v in value)

    return str(value)


def _series_to_csv(rows: Iterable[Iterable[str]]) -> str:
    """rows 를 CSV 문자열로 직렬화."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


# ==========================================
# Public: 노드 CSV
# ==========================================

def build_node_header(columns: list[CsvColumn]) -> str:
    """헤더 한 줄 CSV 문자열 (개행 포함)."""
    return _series_to_csv([[c.header() for c in columns]])


def build_node_data(df: pd.DataFrame, columns: list[CsvColumn]) -> str:
    """헤더 없이 데이터 행만 CSV 문자열로 반환."""
    rows = []
    for _, row in df.iterrows():
        rows.append([
            _format_value(row.get(c.resolve_source()), c)
            for c in columns
        ])
    return _series_to_csv(rows)


def build_node_csv(
    df: pd.DataFrame,
    columns: list[CsvColumn],
) -> tuple[str, str]:
    """노드 CSV 를 (header_csv_text, data_csv_text) 튜플로 반환.

    헤더와 데이터를 분리하는 것은 neo4j-admin import 가
    `--nodes=Label=header.csv,data1.csv,data2.csv` 형태로 여러 데이터 파일을
    같은 헤더에 묶을 수 있도록 하기 위함이다.
    """
    return build_node_header(columns), build_node_data(df, columns)


# ==========================================
# Public: 관계 CSV
# ==========================================

def build_relationship_csv(
    df: pd.DataFrame,
    spec: RelationshipSpec,
) -> tuple[str, str]:
    """관계 CSV 를 (header_csv_text, data_csv_text) 튜플로 반환.

    데이터 행: [start_id, end_id, rel_type, ...property_values]
    """
    header_cells = [
        f":START_ID({spec.start_label})",
        f":END_ID({spec.end_label})",
        ":TYPE",
    ] + [c.header() for c in spec.properties]
    header = _series_to_csv([header_cells])

    rows = []
    for _, row in df.iterrows():
        cells = [
            "" if pd.isna(row.get(spec.start_col)) else str(row[spec.start_col]),
            "" if pd.isna(row.get(spec.end_col))   else str(row[spec.end_col]),
            spec.rel_type,
        ]
        for c in spec.properties:
            cells.append(_format_value(row.get(c.resolve_source()), c))
        rows.append(cells)
    data = _series_to_csv(rows)

    return header, data


# ==========================================
# Public: S3 업로드
# ==========================================

def upload_csv_to_s3(
    csv_text: str,
    bucket: str,
    key: str,
    region: str = "ap-northeast-2",
) -> None:
    """CSV 문자열을 BOM 없는 utf-8 인코딩으로 S3 PutObject.

    Neo4j neo4j-admin import 는 헤더의 BOM 을 자동 strip 하지 않는다.
    BOM 이 있으면 첫 컬럼명이 `\\ufeff{name}` 으로 잘못 파싱되므로
    utf-8-sig 가 아닌 utf-8 을 사용한다.
    """
    body = csv_text.encode("utf-8")
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv; charset=utf-8",
    )
