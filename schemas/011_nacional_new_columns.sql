-- 011_nacional_new_columns.sql
-- DDL Nacional v2: nuevas columnas para importes, documentos, criterios y requisitos.
-- Se aplica sobre todas las tablas nacional_* del schema de trabajo.

DO $$
DECLARE
    tbl TEXT;
    sch TEXT := current_setting('etl.db_schema', true);
BEGIN
    IF sch IS NULL OR sch = '' THEN
        sch := 'l0';
    END IF;

    FOR tbl IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = sch
          AND tablename LIKE 'nacional_%'
    LOOP
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS valor_estimado_contrato NUMERIC(18,2)', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS doc_legal_nombre TEXT', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS doc_legal_url TEXT', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS doc_tecnico_nombre TEXT', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS doc_tecnico_url TEXT', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS docs_adicionales JSONB', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS criterios_adjudicacion JSONB', sch, tbl);
        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS requisitos_solvencia JSONB', sch, tbl);
    END LOOP;
END
$$;
