from __future__ import annotations

from typing import Literal

from smartlogix.ml.query import build_enriched_select_sql

SampleSplit = Literal["train", "validation", "test"]

HASH_BUCKETS = 256
HASH_SEED = "smartlogix-ml-v1"

# Échantillons destinés uniquement au développement local.
#
# Ils correspondent approximativement à :
# train       : 8 / 256 = 3.125 %
# validation  : 26 / 256 = 10.156 %
# test        : 20 / 256 = 7.8125 %
LOCAL_SAMPLE_BUCKETS = {
    "train": 8,
    "validation": 26,
    "test": 20,
}

SPLIT_PREDICATES = {
    "train": (
        "ml.accept_timestamp < "
        "TIMESTAMPTZ '2000-10-01 00:00:00+00'"
    ),
    "validation": (
        "ml.accept_timestamp >= "
        "TIMESTAMPTZ '2000-10-01 00:00:00+00' "
        "AND ml.accept_timestamp < "
        "TIMESTAMPTZ '2000-10-15 00:00:00+00'"
    ),
    "test": (
        "ml.accept_timestamp >= "
        "TIMESTAMPTZ '2000-10-15 00:00:00+00' "
        "AND ml.accept_timestamp < "
        "TIMESTAMPTZ '2000-11-01 00:00:00+00'"
    ),
}


def validate_sample_split(split: str) -> SampleSplit:
    """Valide le nom d'un split ML."""

    if split not in SPLIT_PREDICATES:
        allowed = ", ".join(SPLIT_PREDICATES)
        raise ValueError(
            f"Unknown ML split: {split}. Expected one of: {allowed}"
        )

    return split  # type: ignore[return-value]


def build_hash_sample_predicate(
    *,
    bucket_count: int,
    alias: str = "ml",
) -> str:
    """Construit un échantillonnage stable basé sur order_id.

    MD5 est utilisé ici uniquement pour obtenir une répartition
    déterministe des lignes, pas pour une fonction de sécurité.
    """

    if not 1 <= bucket_count <= HASH_BUCKETS:
        raise ValueError(
            f"bucket_count must be between 1 and {HASH_BUCKETS}"
        )

    return (
        "get_byte("
        f"decode(md5({alias}.order_id::text || ':{HASH_SEED}'), 'hex'), 0"
        f") < {bucket_count}"
    )


def build_local_sample_sql(split: SampleSplit) -> str:
    """Construit un échantillon local reproductible d'un split temporel."""

    validated_split = validate_sample_split(split)
    bucket_count = LOCAL_SAMPLE_BUCKETS[validated_split]

    return f"""
SELECT *
FROM (
    {build_enriched_select_sql()}
) AS ml
WHERE {SPLIT_PREDICATES[validated_split]}
AND {build_hash_sample_predicate(bucket_count=bucket_count)}
ORDER BY ml.accept_timestamp, ml.order_id;
""".strip()