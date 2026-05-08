from oracle_pg_sync.query_perf import build_query_variants, normalize_sql, rewrite_not_in_to_not_exists


def test_query_perf_variants_for_implicit_join_and_not_in():
    sql = """
SELECT DISTINCT a.batch_id || ' : ' || Ori_file_name AS batch_descr, a.batch_id
FROM a_hp_batch a, a_hp_batch_detail b
WHERE a.batch_id = b.batch_id
AND amdocs_created_date IS NOT NULL
AND b.hm_site_id NOT IN (SELECT site_id FROM a_hp_memo_active)
ORDER BY batch_id desc;
"""

    variants = build_query_variants(sql)
    names = {variant.name for variant in variants}

    assert "original" in names
    assert "explicit_join" in names
    assert "not_exists" in names
    assert "explicit_join_not_exists" in names
    explicit = next(variant for variant in variants if variant.name == "explicit_join")
    assert "FROM FROM" not in explicit.sql
    assert "JOIN a_hp_batch_detail b ON a.batch_id = b.batch_id" in explicit.sql


def test_rewrite_not_in_to_not_exists_mentions_outer_column():
    rewritten, changed = rewrite_not_in_to_not_exists(
        "SELECT * FROM detail b WHERE b.hm_site_id NOT IN (SELECT site_id FROM memo_active)"
    )

    assert changed
    assert "NOT EXISTS" in rewritten
    assert "perf_ref_1.site_id = b.hm_site_id" in rewritten


def test_normalize_sql_strips_trailing_semicolon():
    assert normalize_sql("SELECT 1;\n") == "SELECT 1"
