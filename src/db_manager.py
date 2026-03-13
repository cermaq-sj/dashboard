import os
import re
import unicodedata
from collections.abc import Mapping
from urllib.parse import quote

import duckdb
import pandas as pd


class DBManager:
    def __init__(self):
        self.connection_mode = 'local'
        self.connected_db = ':memory:'
        self.connection_error = None
        self.data_revision = 0
        self._cache_revision = -1
        self._simple_cache = {}
        self.con = self._connect()

    def _bump_revision(self):
        self.data_revision += 1

    def _get_cache(self):
        if self._cache_revision != self.data_revision:
            self._simple_cache = {}
            self._cache_revision = self.data_revision
        return self._simple_cache

    def _sanitize_secret(self, value):
        if value is None:
            return None
        text = str(value).strip()
        if len(text) >= 2 and ((text[0] == '"' and text[-1] == '"') or (text[0] == "'" and text[-1] == "'")):
            text = text[1:-1].strip()
        return text or None

    def _find_key_in_mapping(self, data, key: str):
        if not isinstance(data, Mapping):
            return None

        target_lower = key.lower()

        for k, v in data.items():
            if str(k) == key or str(k).lower() == target_lower:
                return v

        for _, v in data.items():
            found = self._find_key_in_mapping(v, key)
            if found is not None:
                return found

        return None

    def _load_secret_value(self, key: str):
        env_val = os.getenv(key)
        if env_val:
            return env_val

        for env_key, env_value in os.environ.items():
            if env_key.lower() == key.lower() and env_value:
                return env_value

        try:
            import streamlit as st
            if key in st.secrets:
                return st.secrets[key]
            nested = self._find_key_in_mapping(st.secrets, key)
            if nested is not None:
                return nested
        except Exception:
            pass

        return None

    def _connect(self):
        token = self._sanitize_secret(
            self._load_secret_value('MOTHERDUCK_TOKEN')
            or self._load_secret_value('MOTHERDUCK_ACCESS_TOKEN')
            or self._load_secret_value('DB_TOKEN')
        )
        database = self._sanitize_secret(
            self._load_secret_value('MOTHERDUCK_DB')
            or self._load_secret_value('MOTHERDUCK_DATABASE')
            or self._load_secret_value('DB_NAME')
            or self._load_secret_value('DB_DATABASE')
        )

        if token:
            errors = []
            token_candidates = []
            raw_token = token.strip()
            encoded_token = quote(raw_token, safe='')
            token_candidates.append(raw_token)
            if encoded_token != raw_token:
                token_candidates.append(encoded_token)

            target_db = database or 'my_db'

            dsn_candidates = []
            for tk in token_candidates:
                dsn_candidates.extend([
                    (f"md:{target_db}?motherduck_token={tk}", target_db),
                    (f"motherduck:{target_db}?motherduck_token={tk}", target_db),
                    (f"md:?motherduck_token={tk}", target_db),
                    (f"motherduck:?motherduck_token={tk}", target_db),
                ])

            for dsn, db_for_use in dsn_candidates:
                con = None
                try:
                    con = duckdb.connect(dsn)

                    if dsn.startswith('md:?') or dsn.startswith('motherduck:?'):
                        try:
                            con.execute(f"CREATE DATABASE IF NOT EXISTS {self._quote_ident(db_for_use)}")
                        except Exception:
                            pass
                        con.execute(f"USE {self._quote_ident(db_for_use)}")

                    self.connection_mode = 'motherduck'
                    self.connected_db = db_for_use
                    self.connection_error = None
                    print(f"Connected to MotherDuck database '{db_for_use}'")
                    return con
                except Exception as e:
                    try:
                        if con is not None:
                            con.close()
                    except Exception:
                        pass
                    errors.append(f"{dsn.split('?')[0]}:{type(e).__name__}:{str(e)[:120]}")

            self.connection_error = ' | '.join(errors)
            print(f"MotherDuck connection failed, falling back to local memory DB: {self.connection_error}")
        else:
            self.connection_error = 'missing_motherduck_token'

        self.connection_mode = 'local'
        self.connected_db = ':memory:'
        return duckdb.connect(database=':memory:')

    def _normalize(self, value) -> str:
        txt = unicodedata.normalize('NFKD', str(value)).encode('ascii', 'ignore').decode('ascii')
        txt = re.sub(r'\s+', ' ', txt.strip().lower())
        return txt

    def _quote_ident(self, identifier: str) -> str:
        return f'"{str(identifier).replace("\"", "\"\"")}"'

    def _find_column(self, columns, exact_norm=None, contains_all=None, excludes_any=None):
        exact_norm = exact_norm or []
        contains_all = contains_all or []
        excludes_any = excludes_any or []

        norm_map = {}
        for c in columns:
            n = self._normalize(c)
            if n not in norm_map:
                norm_map[n] = c

        for candidate in exact_norm:
            found = norm_map.get(self._normalize(candidate))
            if found:
                return found

        if contains_all:
            includes = [self._normalize(x) for x in contains_all]
            excludes = [self._normalize(x) for x in excludes_any]
            for c in columns:
                n = self._normalize(c)
                if all(i in n for i in includes) and not any(e in n for e in excludes):
                    return c

        return None

    def _resolve_columns_by_norm(self, requested_cols, available_cols):
        by_norm = {self._normalize(c): c for c in available_cols}
        resolved = []
        for c in requested_cols:
            if c in available_cols:
                resolved.append(c)
                continue
            n = self._normalize(c)
            if n in by_norm:
                resolved.append(by_norm[n])
                continue
            return None
        return resolved

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.con.execute(f"DESCRIBE {self._quote_ident(table_name)}")
            return True
        except Exception:
            return False

    def _table_row_count(self, table_name: str) -> int:
        if not self._table_exists(table_name):
            return 0
        try:
            return int(self.con.execute(f"SELECT COUNT(*) FROM {self._quote_ident(table_name)}").fetchone()[0])
        except Exception:
            return 0

    def has_any_data(self) -> bool:
        return any(
            self._table_row_count(tbl) > 0
            for tbl in ['fishtalk_data', 'mediciones_data', 'kpi_thresholds', 'proyecciones_data']
        )

    def get_connection_status(self) -> dict:
        return {
            'mode': self.connection_mode,
            'database': self.connected_db,
            'connection_error': self.connection_error,
            'data_revision': self.data_revision,
            'has_data': self.has_any_data(),
            'rows': {
                'fishtalk_data': self._table_row_count('fishtalk_data'),
                'mediciones_data': self._table_row_count('mediciones_data'),
                'kpi_thresholds': self._table_row_count('kpi_thresholds'),
                'proyecciones_data': self._table_row_count('proyecciones_data'),
            },
        }

    def _align_incoming_column_names(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        if not self._table_exists(table_name):
            return df

        target_cols = [r[0] for r in self.con.execute(f"DESCRIBE {self._quote_ident(table_name)}").fetchall()]
        target_norm_map = {self._normalize(c): c for c in target_cols}

        rename_map = {}
        current_cols = set(df.columns)
        for col in df.columns:
            if col in target_cols:
                continue
            mapped = target_norm_map.get(self._normalize(col))
            if mapped and mapped not in current_cols:
                rename_map[col] = mapped
                current_cols.add(mapped)

        if rename_map:
            return df.rename(columns=rename_map)
        return df

    def _ensure_table_has_incoming_columns(self, table_name: str, incoming_df: pd.DataFrame):
        desc_target = self.con.execute(f"DESCRIBE {self._quote_ident(table_name)}").fetchall()
        target_cols = {r[0] for r in desc_target}

        self.con.register('__incoming_schema_view', incoming_df)
        try:
            desc_in = self.con.execute("DESCRIBE __incoming_schema_view").fetchall()
            incoming_types = {r[0]: r[1] for r in desc_in}
        finally:
            self.con.unregister('__incoming_schema_view')

        for col, col_type in incoming_types.items():
            if col not in target_cols:
                self.con.execute(
                    f"ALTER TABLE {self._quote_ident(table_name)} ADD COLUMN {self._quote_ident(col)} {col_type}"
                )

    def _convert_numeric_columns(self, df: pd.DataFrame, protected_columns=None):
        protected_columns = protected_columns or []
        protected_norm = {self._normalize(c) for c in protected_columns}

        text_markers = [
            'sheet', 'source', 'lugar', 'horario', 'muestreo', 'departamento',
            'depto', 'unidad', 'batch', 'lote', 'parametro', 'estado',
            'riesgo', 'explicacion', 'medidor', 'vacuna', 'comportamiento',
            'ayuno', 'biofiltros', 'hora', 'cliente'
        ]

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue

            col_norm = self._normalize(col)
            if col_norm in protected_norm:
                continue
            if any(marker in col_norm for marker in text_markers):
                continue

            if df[col].dtype == 'object':
                converted = pd.to_numeric(df[col], errors='coerce')
                if converted.notna().sum() > 0:
                    df[col] = converted.astype(float)
            elif pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].astype(float)

        return df

    def _build_stage_from_incoming(self, incoming_df: pd.DataFrame, key_columns, stage_name: str, target_schema=None):
        self.con.register('__incoming_upsert_view', incoming_df)
        try:
            if target_schema is None:
                select_expr = ', '.join(self._quote_ident(c) for c in incoming_df.columns)
            else:
                target_cols = [c for c, _ in target_schema]
                target_types = {c: t for c, t in target_schema}
                incoming_cols = set(incoming_df.columns)

                exprs = []
                for col in target_cols:
                    qcol = self._quote_ident(col)
                    if col in incoming_cols:
                        exprs.append(f"TRY_CAST({qcol} AS {target_types[col]}) AS {qcol}")
                    else:
                        exprs.append(f"CAST(NULL AS {target_types[col]}) AS {qcol}")
                select_expr = ', '.join(exprs)

            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE {stage_name}_raw AS "
                f"SELECT {select_expr}, ROW_NUMBER() OVER () AS \"__ord\" FROM __incoming_upsert_view"
            )

            key_sql = ', '.join(self._quote_ident(c) for c in key_columns)
            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE {stage_name} AS "
                f"SELECT * EXCLUDE (\"__ord\", \"__rn\") "
                f"FROM ("
                f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_sql} ORDER BY \"__ord\" DESC) AS \"__rn\" "
                f"  FROM {stage_name}_raw"
                f") t "
                f"WHERE \"__rn\" = 1"
            )
        finally:
            self.con.unregister('__incoming_upsert_view')

    def _upsert_dataframe(self, df: pd.DataFrame, table_name: str, key_columns, replace_on_keys=None):
        summary = {
            'received': 0,
            'dropped_null_keys': 0,
            'table': table_name,
        }

        if df is None or df.empty:
            return summary

        work_df = df.copy()
        work_df.columns = [str(c).strip() for c in work_df.columns]
        summary['received'] = len(work_df)

        key_columns = self._resolve_columns_by_norm(key_columns, list(work_df.columns))
        if not key_columns:
            raise ValueError(f"No se pudieron resolver columnas clave para {table_name}: {key_columns}")

        for key_col in key_columns:
            if work_df[key_col].dtype == 'object':
                work_df[key_col] = work_df[key_col].astype(str).str.strip().replace('', pd.NA)

        valid_mask = work_df[key_columns].notna().all(axis=1)
        dropped = int((~valid_mask).sum())
        if dropped > 0:
            summary['dropped_null_keys'] = dropped
            work_df = work_df[valid_mask].copy()

        if work_df.empty:
            return summary

        stage_name = '__stage_upsert'
        table_exists = self._table_exists(table_name)

        if table_exists:
            work_df = self._align_incoming_column_names(work_df, table_name)
            self._ensure_table_has_incoming_columns(table_name, work_df)

            target_schema = [(r[0], r[1]) for r in self.con.execute(f"DESCRIBE {self._quote_ident(table_name)}").fetchall()]
            target_cols = [c for c, _ in target_schema]

            resolved_keys = self._resolve_columns_by_norm(key_columns, target_cols)
            if not resolved_keys:
                raise ValueError(f"No se pudieron mapear llaves contra tabla destino {table_name}")

            self._build_stage_from_incoming(work_df, resolved_keys, stage_name, target_schema=target_schema)

            if replace_on_keys:
                replace_keys = self._resolve_columns_by_norm(replace_on_keys, target_cols)
                if replace_keys:
                    rk_sql = ', '.join(self._quote_ident(c) for c in replace_keys)
                    delete_on = ' AND '.join(
                        f"tgt.{self._quote_ident(k)} IS NOT DISTINCT FROM ev.{self._quote_ident(k)}"
                        for k in replace_keys
                    )
                    self.con.execute(
                        f"DELETE FROM {self._quote_ident(table_name)} AS tgt "
                        f"USING (SELECT DISTINCT {rk_sql} FROM {stage_name}) AS ev "
                        f"WHERE {delete_on}"
                    )

            incoming_norms = {self._normalize(c) for c in work_df.columns}
            mutable_cols = [
                c for c in target_cols
                if c not in resolved_keys and self._normalize(c) in incoming_norms
            ]

            on_clause = ' AND '.join(
                f"tgt.{self._quote_ident(k)} IS NOT DISTINCT FROM src.{self._quote_ident(k)}"
                for k in resolved_keys
            )

            merge_parts = [
                f"MERGE INTO {self._quote_ident(table_name)} AS tgt",
                f"USING {stage_name} AS src",
                f"ON {on_clause}",
            ]

            if mutable_cols:
                change_cond = ' OR '.join(
                    f"tgt.{self._quote_ident(c)} IS DISTINCT FROM src.{self._quote_ident(c)}"
                    for c in mutable_cols
                )
                set_clause = ', '.join(
                    f"{self._quote_ident(c)} = src.{self._quote_ident(c)}"
                    for c in mutable_cols
                )
                merge_parts.append(f"WHEN MATCHED AND ({change_cond}) THEN UPDATE SET {set_clause}")

            insert_cols = ', '.join(self._quote_ident(c) for c in target_cols)
            insert_vals = ', '.join(f"src.{self._quote_ident(c)}" for c in target_cols)
            merge_parts.append(f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")

            self.con.execute('\n'.join(merge_parts))

        else:
            self._build_stage_from_incoming(work_df, key_columns, stage_name, target_schema=None)
            self.con.execute(
                f"CREATE TABLE {self._quote_ident(table_name)} AS SELECT * FROM {stage_name}"
            )

        self.con.execute(f"DROP TABLE IF EXISTS {stage_name}_raw")
        self.con.execute(f"DROP TABLE IF EXISTS {stage_name}")

        return summary

    def _resolve_production_key_columns(self, columns):
        col_fecha = self._find_column(columns, exact_norm=['final fecha'], contains_all=['final', 'fecha'])
        col_batch = self._resolve_col('Lote', columns)
        col_dept = self._resolve_col('Departamento', columns)
        col_unit = self._resolve_col('Unidad', columns)
        col_days = self._find_column(
            columns,
            exact_norm=['final days since first input'],
            contains_all=['final', 'days since first input'],
        )
        col_final_num = self._find_column(
            columns,
            exact_norm=['final numero'],
            contains_all=['final', 'numero'],
            excludes_any=['mortalidad', 'ventas', 'eliminados', 'perdida'],
        )

        key_cols = [col_fecha, col_batch, col_dept, col_unit, col_days, col_final_num]
        if all(key_cols):
            return key_cols
        return None

    def ingest_data(self, df: pd.DataFrame, table_name: str = 'fishtalk_data'):
        """
        Ingesta del Excel maestro (pesado) con upsert por clave de negocio:
        Final Fecha + Batch/Lote + Departamento + Unidad +
        Final Days since first input + Final Número.
        """
        if df is None or df.empty:
            return {'received': 0, 'dropped_null_keys': 0, 'table': table_name}

        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        if table_name == 'fishtalk_data':
            final_num_col = self._find_column(
                df.columns,
                exact_norm=['final numero'],
                contains_all=['final', 'numero'],
                excludes_any=['mortalidad', 'ventas', 'eliminados', 'perdida'],
            )

            if final_num_col:
                initial_len = len(df)
                vals = pd.to_numeric(df[final_num_col], errors='coerce')
                df = df[(vals != 0) & (vals.notna())].copy()
                if len(df) < initial_len:
                    print(f"Excluded {initial_len - len(df)} rows with invalid Final Número (Production only)")

            batch_col = self._resolve_col('Lote', list(df.columns))
            fecha_col = self._find_column(df.columns, exact_norm=['final fecha'], contains_all=['final', 'fecha'])
            biomasa_col = self._find_column(
                df.columns,
                exact_norm=['final biomasa'],
                contains_all=['biomasa'],
            )
            dept_col = self._resolve_col('Departamento', list(df.columns))
            unit_col = self._resolve_col('Unidad', list(df.columns))

            if batch_col and fecha_col and biomasa_col:
                try:
                    import numpy as np

                    temp_fecha = pd.to_datetime(df[fecha_col], errors='coerce')
                    temp_biomasa = pd.to_numeric(df[biomasa_col], errors='coerce')

                    valid_df = pd.DataFrame({
                        'batch': df[batch_col],
                        'unit': df[unit_col] if unit_col else 'UNKNOWN_UNIT',
                        'fecha': temp_fecha,
                        'biomasa': temp_biomasa,
                    })

                    if dept_col:
                        valid_df['dept'] = df[dept_col].astype(str).str.strip().str.upper()
                    else:
                        valid_df['dept'] = 'UNKNOWN'

                    fry_df = valid_df[(valid_df['dept'] == 'FRY') & (valid_df['biomasa'] > 0)].dropna(subset=['fecha', 'biomasa'])

                    initial_biomasa_map = {}
                    if not fry_df.empty:
                        fry_df = fry_df.sort_values('fecha')
                        first_per_unit = fry_df.groupby(['batch', 'unit']).first().reset_index()

                        def avg_without_outliers(series):
                            vals = series.values
                            if len(vals) == 0:
                                return np.nan
                            if len(vals) < 3:
                                return np.mean(vals)

                            q1 = np.percentile(vals, 25)
                            q3 = np.percentile(vals, 75)
                            iqr = q3 - q1
                            lower = q1 - 1.5 * iqr
                            upper = q3 + 1.5 * iqr

                            normals = vals[(vals >= lower) & (vals <= upper)]
                            if len(normals) == 0:
                                return np.mean(vals)
                            return np.mean(normals)

                        initial_biomasa_map = first_per_unit.groupby('batch')['biomasa'].apply(avg_without_outliers).to_dict()

                    baseline = df[batch_col].map(initial_biomasa_map)
                    df['Dif biomasa'] = (temp_biomasa - baseline).abs()

                    mort_col = 'Final Mortalidad, Biomasa' if 'Final Mortalidad, Biomasa' in df.columns else None
                    temp_mort = pd.to_numeric(df[mort_col], errors='coerce').fillna(0) if mort_col else 0
                    df['Dif biomasa + bio mort'] = ((temp_biomasa - baseline) + temp_mort).abs()

                except Exception as e:
                    print(f"Error calculating 'Dif biomasa': {e}")
                    df['Dif biomasa'] = pd.NA
                    df['Dif biomasa + bio mort'] = pd.NA

        self._convert_numeric_columns(df)

        key_cols = self._resolve_production_key_columns(list(df.columns))
        if not key_cols:
            raise ValueError(
                "No se pudieron resolver todas las columnas clave del Excel maestro "
                "(Final Fecha, Batch/Lote, Departamento, Unidad, Final Days since first input, Final Número)."
            )

        summary = self._upsert_dataframe(df, table_name=table_name, key_columns=key_cols)
        if summary.get('received', 0) > 0:
            self._bump_revision()
        return summary

    def ingest_mediciones_data(self, file, table_name: str = 'mediciones_data'):
        """
        Ingesta de Mediciones con claves por hoja:
        - Hatchery: sheet_name + Fecha
        - Alevinaje: sheet_name + Day (normalizado a Fecha)
        - Smolt: sheet_name + Day (normalizado a Fecha)
        - Metales: sheet_name + Fecha + Horario + Lugar de muestreo
        - Se excluyen i-STAT y Alertas por Estado por rendimiento
        """
        try:
            sheets_dict = pd.read_excel(file, sheet_name=None)
        except Exception:
            file.seek(0)
            sheets_dict = pd.read_excel(file, sheet_name=None)

        total_rows = 0
        sheet_summaries = {}
        excluded_sheets = {'i-stat', 'i stat', 'alertas por estado'}

        # Cleanup legacy rows for excluded sheets if they exist in persisted table.
        if self._table_exists(table_name):
            try:
                existing_cols = [c[0] for c in self.con.execute(f"DESCRIBE {self._quote_ident(table_name)}").fetchall()]
                if 'sheet_name' in existing_cols:
                    self.con.execute(
                        f"DELETE FROM {self._quote_ident(table_name)} "
                        "WHERE LOWER(TRIM(sheet_name)) IN ('i-stat', 'i stat', 'alertas por estado')"
                    )
            except Exception:
                pass

        for sheet_name, raw_df in sheets_dict.items():
            if raw_df is None or raw_df.empty:
                continue

            df = raw_df.copy()
            df.columns = [str(c).strip() for c in df.columns]
            sheet_norm = self._normalize(sheet_name)

            if sheet_norm in excluded_sheets:
                continue

            df['sheet_name'] = sheet_name
            df['source_file'] = 'Mediciones'

            replace_on_keys = None

            if sheet_norm in ('hatchery', 'alevinaje', 'smolt', 'metales'):
                if sheet_norm in ('alevinaje', 'smolt'):
                    date_col = self._find_column(df.columns, exact_norm=['day'], contains_all=['day'])
                else:
                    date_col = self._find_column(df.columns, exact_norm=['fecha'], contains_all=['fecha'])

                if not date_col:
                    date_col = self._find_column(df.columns, contains_all=['date'])

                if date_col and date_col != 'Fecha':
                    df = df.rename(columns={date_col: 'Fecha'})
                if 'Fecha' in df.columns:
                    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')

                lugar_col = self._find_column(
                    df.columns,
                    exact_norm=['lugar de muestreo'],
                    contains_all=['lugar', 'muestreo'],
                )
                if lugar_col and lugar_col != 'Lugar de muestreo':
                    df = df.rename(columns={lugar_col: 'Lugar de muestreo'})
                if 'Lugar de muestreo' not in df.columns:
                    df['Lugar de muestreo'] = 'General'

                if sheet_norm == 'metales':
                    horario_col = self._find_column(df.columns, exact_norm=['horario'], contains_all=['horario'])
                    if horario_col and horario_col != 'Horario':
                        df = df.rename(columns={horario_col: 'Horario'})
                    key_cols = ['sheet_name', 'Fecha', 'Horario', 'Lugar de muestreo']
                else:
                    key_cols = ['sheet_name', 'Fecha']

            else:
                generic_date = self._find_column(df.columns, exact_norm=['fecha'], contains_all=['fecha'])
                if generic_date and generic_date != 'Fecha':
                    df = df.rename(columns={generic_date: 'Fecha'})
                if 'Fecha' in df.columns:
                    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
                key_cols = ['sheet_name', 'Fecha'] if 'Fecha' in df.columns else ['sheet_name']

            self._convert_numeric_columns(df, protected_columns=key_cols)

            summary = self._upsert_dataframe(
                df,
                table_name=table_name,
                key_columns=key_cols,
                replace_on_keys=replace_on_keys,
            )
            sheet_summaries[sheet_name] = summary
            total_rows += summary.get('received', 0)

        print(f"Successfully ingested/updated {total_rows} rows from Mediciones across {len(sheet_summaries)} sheets")
        if total_rows > 0:
            self._bump_revision()
        return {'total_received': total_rows, 'sheets': sheet_summaries}

    def query(self, sql: str) -> pd.DataFrame:
        """
        Executes a raw SQL query and returns a pandas DataFrame.
        """
        try:
            sql_stripped = sql.strip()
            sql_norm = ' '.join(sql_stripped.split()).lower()
            cacheable = (
                sql_norm.startswith('describe ')
                or sql_norm.startswith('show ')
                or sql_norm.startswith('select distinct ')
                or sql_norm.startswith('select min(')
                or sql_norm.startswith('select max(')
            )

            if cacheable:
                cache = self._get_cache()
                cache_key = ('query', sql_norm)
                if cache_key in cache:
                    return cache[cache_key].copy(deep=False)

            df = self.con.execute(sql).df()

            if cacheable and len(df) <= 5000:
                self._get_cache()[('query', sql_norm)] = df

            return df
        except Exception as e:
            print(f"SQL Error: {e}")
            return pd.DataFrame()

    def _resolve_col(self, key: str, cols: list) -> str:
        """
        Helper to map standard names to actual columns.
        """
        # Heuristics for column mapping
        mappings = {
            'Fecha': ['final fecha', 'fecha', 'date'],
            'Lote': ['lote', 'batch', 'group'],
            'Departamento': ['depto', 'dep', 'area', 'sect'],
            'Unidad': ['unidad', 'unit', 'jaula', 'cage'],
            # Prioritize 'primer ingreso' as per user request
            'Days': ['final days since first input', 'primer ingreso', 'first input', 'dias', 'days', 'input'],
            'Week': ['semana', 'week']
        }
        candidates = mappings.get(key, [key])
        
        # 1. Exact match
        for cand in candidates:
             if cand in cols:
                 return cand
                 
        # 2. Case insensitive match
        for c in cols:
             if any(cand.lower() == c.lower() for cand in candidates):
                 return c
                 
        # 3. Substring match
        for c in cols:
            if any(cand.lower() in c.lower() for cand in candidates):
                return c
        return None

    def get_summary(self, table_name: str = 'fishtalk_data'):
        """
        Returns a summary dictionary of the data in the table.
        """
        try:
            # Check if table exists
            tables = self.con.execute("SHOW TABLES").fetchall()
            if not tables or (table_name,) not in tables:
                return None
            
            # Get valid columns to construct query dynamically
            columns_info = self.con.execute(f"DESCRIBE {table_name}").fetchall()
            col_names = [c[0] for c in columns_info]
            
            date_col = self._resolve_col('Fecha', col_names)
            batch_col = self._resolve_col('Lote', col_names)
            unit_col = self._resolve_col('Unidad', col_names)
            dept_col = self._resolve_col('Departamento', col_names)
            
            # Construct the query parts
            select_parts = ["COUNT(*) as total_rows"]
            
            if date_col:
                select_parts.append(f'MIN("{date_col}") as min_date')
                select_parts.append(f'MAX("{date_col}") as max_date')
            else:
                select_parts.append("NULL as min_date, NULL as max_date")
                
            if batch_col:
                select_parts.append(f'COUNT(DISTINCT "{batch_col}") as total_batches')
            else:
                select_parts.append("0 as total_batches")

            if unit_col:
                select_parts.append(f'COUNT(DISTINCT "{unit_col}") as total_units')
            else:
                select_parts.append("0 as total_units")
                
            if dept_col:
                select_parts.append(f'COUNT(DISTINCT "{dept_col}") as total_departments')
            else:
                select_parts.append("0 as total_departments")
            
            query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
            
            result = self.con.execute(query).fetchone()
            
            # Map result to dictionary
            summary_keys = ['total_rows', 'min_date', 'max_date', 'total_batches', 'total_units', 'total_departments']
            return dict(zip(summary_keys, result))
            
        except Exception as e:
            print(f"Error in get_summary: {e}")
            return None

    def get_unique_values(self, col_name: str, table_name: str = 'fishtalk_data'):
        """Get unique values for a specific column."""
        try:
            cache_key = ('unique_values', table_name, str(col_name).lower())
            cache = self._get_cache()
            if cache_key in cache:
                return list(cache[cache_key])

            # Check if column exists (case insensitive)
            cols = [c[0] for c in self.con.execute(f"DESCRIBE {table_name}").fetchall()]
            resolved_col = self._resolve_col(col_name, cols)
            
            if not resolved_col:
                return []

            values = [
                row[0]
                for row in self.con.execute(
                    f'SELECT DISTINCT "{resolved_col}" FROM {table_name} WHERE "{resolved_col}" IS NOT NULL ORDER BY 1'
                ).fetchall()
            ]
            cache[cache_key] = tuple(values)
            return values
        except Exception as e:
            print(f"Error getting unique values for {col_name}: {e}")
            return []

    def get_min_max(self, column_name: str, table_name: str = 'fishtalk_data'):
        """
        Get min and max values for a column.
        """
        try:
            cache_key = ('min_max', table_name, str(column_name).lower())
            cache = self._get_cache()
            if cache_key in cache:
                return cache[cache_key]

            cols = [c[0] for c in self.con.execute(f"DESCRIBE {table_name}").fetchall()]
            col_match = self._resolve_col(column_name, cols)
            
            if not col_match:
                return None, None
                
            query = f'SELECT MIN("{col_match}"), MAX("{col_match}") FROM {table_name}'
            min_val, max_val = self.con.execute(query).fetchone()
            cache[cache_key] = (min_val, max_val)
            return min_val, max_val
        except Exception as e:
            print(f"Error getting min/max for {column_name}: {e}")
            return None, None

            
            # 3. Variables & Aggregation
            selected_vars = filters.get('mediciones_vars', [])
            if not selected_vars:
                 return pd.DataFrame()
                 
            # Build Selects
            group_keys = [f'"{col_date}"', 'sheet_name', f'"{col_lugar}"']
            
            metric_selects = []
            for var in selected_vars:
                # Resolve column (substring match similar to other logic?)
                # We can use direct match if they came from our metadata list
                matched_col = next((c for c in cols if c == var), None)
                if not matched_col:
                     matched_col = next((c for c in cols if c.lower() == var.lower()), None)
                
                if matched_col:
                    # AVG aggregation for chart
                    # Handle non-numeric gracefully? Data cleaning should have handled it.
                    metric_selects.append(f'AVG("{matched_col}") as "{var}"')

            if not metric_selects:
                return pd.DataFrame()
            
            select_sql = ", ".join(group_keys + metric_selects)
            group_sql = ", ".join(group_keys)
            
            query = f"""
                SELECT {select_sql}
                FROM {table_name}
                WHERE {where_sql}
                GROUP BY {group_sql}
                ORDER BY sheet_name, "{col_lugar}", "{col_date}"
            """
            
            return self.con.execute(query).df()
            
        except Exception as e:
            print(f"Error in get_mediciones_chart_data: {e}")
            import traceback; traceback.print_exc()
            return pd.DataFrame()

    def get_mediciones_metadata(self, table_name: str = 'mediciones_data'):
        """
        Retrieves metadata for Mediciones files:
        - Distinct sheets found in files with 'Mediciones' in name
        - For each sheet, distinct 'Lugar de muestreo'
        - For each sheet, available numeric columns (variables)
        """
        try:
            cols_info = self.con.execute(f"DESCRIBE {table_name}").fetchall()
            cols = [c[0] for c in cols_info]
            if 'sheet_name' not in cols:
                return {}
            
            # Get sheets
            sheets = [
                r[0] for r in self.con.execute(
                    f"""
                    SELECT DISTINCT sheet_name
                    FROM {table_name}
                    WHERE sheet_name IS NOT NULL
                      AND LOWER(TRIM(sheet_name)) NOT IN ('i-stat', 'alertas por estado')
                    """
                ).fetchall()
            ]
            
            # Identify numeric columns for variable detection
            desc = self.con.execute(f"DESCRIBE {table_name}").df()
            numeric_types = ['DOUBLE', 'FLOAT', 'DECIMAL', 'BIGINT', 'INTEGER', 'INT', 'HUGEINT', 'SMALLINT', 'TINYINT']
            numeric_cols = desc[desc['column_type'].str.upper().isin(numeric_types)]['column_name'].tolist()
            
            # Exclude structural
            structural = ['Lugar de muestreo', 'Fecha', 'Days', 'sheet_name', 'source_file', 'index', 'Horario']
            potential_vars = [c for c in numeric_cols if c not in structural]

            metadata = {}
            col_lugar = self._resolve_col('Lugar de muestreo', cols)
            
            for sheet in sheets:
                sheet_meta = {'places': [], 'columns': []}
                
                # Places
                if col_lugar:
                    places_q = f"SELECT DISTINCT \"{col_lugar}\" FROM {table_name} WHERE sheet_name = ? AND \"{col_lugar}\" IS NOT NULL ORDER BY 1"
                    places = sorted([str(r[0]) for r in self.con.execute(places_q, [sheet]).fetchall() if pd.notna(r[0])])
                    sheet_meta['places'] = places
                
                # Variables (Numeric columns that have non-null values for this specific sheet)
                if potential_vars:
                    aggs = [f"COUNT(\"{c}\")" for c in potential_vars]
                    agg_q = f"SELECT {', '.join(aggs)} FROM {table_name} WHERE sheet_name = ?"
                    counts = self.con.execute(agg_q, [sheet]).fetchone()
                    
                    valid_vars = [var for var, count in zip(potential_vars, counts) if count > 0]
                    sheet_meta['columns'] = sorted(valid_vars)
                
                metadata[sheet] = sheet_meta
                
            return metadata
            
        except Exception as e:
            print(f"Error extracting Mediciones metadata: {e}")
            return {}
            
    def get_mediciones_chart_data(self, filters: dict, table_name: str = 'mediciones_data'):
        """
        Specific query for Mediciones Chart.
        - Source: Only files with 'Mediciones' in name
        - Group By: Date, Sheet Name, Lugar de muestreo
        - Aggregation: AVG for numeric variables (or split by Horario if not averaging)
        """
        try:
            # 1. Columns
            cols = [c[0] for c in self.con.execute(f"DESCRIBE {table_name}").fetchall()]
            
            # For Mediciones, the date column is "Fecha"
            col_date = next((c for c in cols if c.lower() == 'fecha'), None)
            col_lugar = self._resolve_col('Lugar de muestreo', cols)
            col_horario = next((c for c in cols if 'horario' in c.lower()), None)
            
            if not col_date or not col_lugar:
                return pd.DataFrame()

            # 2. Filters
            where_clauses = [
                "source_file ILIKE '%Mediciones%'",
                "LOWER(TRIM(sheet_name)) NOT IN ('i-stat', 'alertas por estado')",
            ]
            
            # Date Range
            if filters.get('mediciones_date_range'):
                dr = filters['mediciones_date_range']
                if len(dr) == 2:
                    start = pd.to_datetime(dr[0]).strftime('%Y-%m-%d')
                    end = pd.to_datetime(dr[1]).strftime('%Y-%m-%d')
                    where_clauses.append(f'"{col_date}" BETWEEN \'{start}\' AND \'{end}\'')
            
            # Places
            med_places = filters.get('mediciones_places', [])
            if med_places:
                 places_str = "', '".join([str(p).replace("'", "''") for p in med_places])
                 where_clauses.append(f'"{col_lugar}" IN (\'{places_str}\')')
            else:
                return pd.DataFrame()

            where_sql = " AND ".join(where_clauses)
            
            # 3. Variables & Aggregation
            selected_vars = filters.get('mediciones_vars', [])
            if not selected_vars:
                 return pd.DataFrame()
                 
            avg_mode = filters.get('mediciones_avg', False)
            group_keys = [f'"{col_date}"', 'sheet_name', f'"{col_lugar}"']
            select_keys = [f'"{col_date}"', 'sheet_name', f'"{col_lugar}"']
            
            # Include Horario if exists
            if col_horario:
                if avg_mode:
                    # If averaging, we aggregate Horario using ANY_VALUE or MAX (so we don't group by it)
                    select_keys.append(f'MAX("{col_horario}") as "{col_horario}"')
                else:
                    group_keys.append(f'"{col_horario}"')
                    select_keys.append(f'"{col_horario}"')
            
            metric_selects = []
            for var in selected_vars:
                # 1. Exact match
                matched_col = next((c for c in cols if c == var), None)
                if not matched_col:
                     matched_col = next((c for c in cols if c.strip().lower() == var.strip().lower()), None)
                if not matched_col:
                     matched_col = next((c for c in cols if var.lower() in c.lower()), None)

                if matched_col:
                    metric_selects.append(f'AVG("{matched_col}") as "{var}"')

            if not metric_selects:
                return pd.DataFrame()
            
            select_sql = ", ".join(select_keys + metric_selects)
            group_sql = ", ".join(group_keys)
            
            query = f"""
                SELECT {select_sql}
                FROM {table_name}
                WHERE {where_sql}
                GROUP BY {group_sql}
                ORDER BY sheet_name, "{col_lugar}", "{col_date}"
            """
            
            return self.con.execute(query).df()
            
        except Exception as e:
            print(f"Error in get_mediciones_chart_data: {e}")
            import traceback; traceback.print_exc()
            return pd.DataFrame()

    def get_mediciones_date_range(self, table_name: str = 'mediciones_data'):
        """Get min/max date for Mediciones files."""
        try:
            cols = [c[0] for c in self.con.execute(f"DESCRIBE {table_name}").fetchall()]
            col_date = next((c for c in cols if c.lower() == 'fecha'), None)
            if not col_date:
                col_date = next((c for c in cols if 'fecha' in c.lower() and 'final' not in c.lower()), None)
            
            if not col_date: 
                return None, None
                
            query = (
                f"SELECT MIN(\"{col_date}\"), MAX(\"{col_date}\") "
                f"FROM {table_name} "
                "WHERE source_file ILIKE '%Mediciones%' "
                "AND LOWER(TRIM(sheet_name)) NOT IN ('i-stat', 'alertas por estado')"
            )
            return self.con.execute(query).fetchone()
        except:
             return None, None
             
    def get_min_max(self, column_name: str, table_name: str = 'fishtalk_data'):
        """
        Get min and max values for a column.
        """
        try:
            cache_key = ('min_max', table_name, str(column_name).lower())
            cache = self._get_cache()
            if cache_key in cache:
                return cache[cache_key]

            cols = [c[0] for c in self.con.execute(f"DESCRIBE {table_name}").fetchall()]
            col_match = self._resolve_col(column_name, cols)
            
            if not col_match:
                return None, None
                
            query = f'SELECT MIN("{col_match}"), MAX("{col_match}") FROM {table_name}'
            min_val, max_val = self.con.execute(query).fetchone()
            cache[cache_key] = (min_val, max_val)
            return min_val, max_val
        except Exception:
            return None, None
            
    def get_filtered_data(self, filters: dict, table_name: str = 'fishtalk_data'):
        """
        Execute dynamic query based on filters.
        
        filters dict keys:
        - batches: list of selected batches
        - depts: list of selected departments
        - units: list of selected units
        - date_range: tuple (start, end)
        - days_range: tuple (min, max)
        - variables: list of variables to select
        - sum_units: boolean
        """
        try:
            # 1. Inspect Columns
            cols = [c[0] for c in self.con.execute(f"DESCRIBE {table_name}").fetchall()]
            
            col_date = self._resolve_col('Fecha', cols)
            col_lote = self._resolve_col('Lote', cols)
            col_dept = self._resolve_col('Departamento', cols)
            col_unit = self._resolve_col('Unidad', cols)
            col_days = self._resolve_col('Days', cols)

            # 2. Build WHERE Clauses
            # Logic:
            # Common Filters: Date Range, Days Range (Applies to ALL)
            # Main Filters: Batches, Depts, Units (Applies to Standard Data)
            # Mediciones Filters: Places (Applies to Mediciones Data)
            
            common_where = []
            
            # Dates
            if filters.get('date_range') and col_date:
                dr = filters['date_range']
                if len(dr) == 2:
                    start_date = pd.to_datetime(dr[0]).strftime('%Y-%m-%d')
                    end_date = pd.to_datetime(dr[1]).strftime('%Y-%m-%d')
                    common_where.append(f'"{col_date}" BETWEEN \'{start_date}\' AND \'{end_date}\'')

            # Days
            if filters.get('days_range') and col_days:
                 common_where.append(f'"{col_days}" BETWEEN {filters["days_range"][0]} AND {filters["days_range"][1]}')
            
            common_sql = " AND ".join(common_where) if common_where else "1=1"
            
            # Parameter Range Filters (from Config tab)
            param_ranges = filters.get('param_ranges', {})
            range_where = []
            for col_name, (rmin, rmax) in param_ranges.items():
                # Verify column exists in this table
                if col_name in cols or any(c for c in cols if c.lower() == col_name.lower()):
                    matched = next((c for c in cols if c == col_name or c.lower() == col_name.lower()), None)
                    if matched:
                        range_where.append(f'"{matched}" BETWEEN {rmin} AND {rmax}')
            
            if range_where:
                common_sql = common_sql + " AND " + " AND ".join(range_where)
            
            # --- Branch 1: Main Data Filters ---
            main_where = []
            if filters.get('batches') and col_lote:
                batches_str = "', '".join([str(b) for b in filters['batches']])
                main_where.append(f'"{col_lote}" IN (\'{batches_str}\')')
                
            if filters.get('depts') and col_dept:
                depts_str = "', '".join([str(d) for d in filters['depts']])
                main_where.append(f'"{col_dept}" IN (\'{depts_str}\')')

            if filters.get('units') and col_unit:
                units_str = "', '".join([str(u) for u in filters['units']])
                main_where.append(f'"{col_unit}" IN (\'{units_str}\')')
            
            # To avoid "Main Filters" accidentally matching Mediciones data that we want to control separately,
            # we generally enforce that Main Data excludes Mediciones source file IF we need strict separation.
            # But usually, Main Data just won't match "Lote" if it's null in Mediciones.
            # However, to be safe and explicit:
            # Main Branch = (Matches Filters) AND (Source NOT LIKE 'Mediciones')? 
            # Or just (Matches Filters). Let's stick to Matches Filters.
            # If Mediciones rows happen to have matching Batches, they will show up.
            
            main_sql = " AND ".join(main_where) if main_where else "1=1"
            if not main_where:
                # If no main filters selected, usually we show everything? 
                # Or if app defaults to select all batches, then main_where is populated.
                pass

            # --- Branch 2: Mediciones Data Filters ---
            med_places = filters.get('mediciones_places', [])
            col_lugar = self._resolve_col('Lugar de muestreo', cols)
            
            med_sql = "0=1" # Default to false if no mediciones logic applies
            
            if med_places and col_lugar:
                # If places selected: Show rows from Mediciones files AND matching places
                places_str = "', '".join([str(p) for p in med_places])
                med_sql = f"(source_file ILIKE '%Mediciones%' AND \"{col_lugar}\" IN ('{places_str}'))"
            elif col_lugar:
                # If NO places selected, do we show ALL mediciones or NONE?
                # User request: "Quiero que actue solo, pero sí se pueda colocar en el mismo gráfico"
                # "Y quiero que de la lista se puedan seleccionar uno o más"
                # If nothing selected, typically don't show the separate data to keep chart clean?
                # Or if user selected VARIABLES but no places?
                # Let's assume: If user selects Variables, they will presumably select Places.
                # If we show all by default, it might be too much.
                # Let's make it exclusive: must select place to see explicit mediciones data.
                pass

            # --- Combine ---
            # Query = Common AND (Main OR Med)
            # If Main is filtering (e.g. Batch='A'), we normally see data for 'A'.
            # If we add Med place='X', we want to see 'A' + 'X'.
            
            # Corner case: If NO main filters (e.g. first load), main_sql is "1=1" -> Shows everything.
            # If Main Filters are Active: main_sql is specific.
            
            where_sql = f"({common_sql}) AND ( ({main_sql}) OR ({med_sql}) )"

            # Dynamic Acumulado calculations (FCR, GF3, SGR)
            col_fcr_eco = next((c for c in cols if c.strip().lower() == 'final fcr económico' or c.strip().lower() == 'final fcr economico'), None)
            col_fcr_bio = next((c for c in cols if c.strip().lower() == 'final fcr biológico' or c.strip().lower() == 'final fcr biologico'), None)
            col_dif_bio = 'Dif biomasa' if 'Dif biomasa' in cols else None
            col_dif_bio_mort = 'Dif biomasa + bio mort' if 'Dif biomasa + bio mort' in cols else col_dif_bio
            col_gf3 = next((c for c in cols if c.strip().lower() == 'final gf3'), None)
            col_sgr = next((c for c in cols if c.strip().lower() == 'final sgr'), None)
            col_sfr = next((c for c in cols if c.strip().lower() == 'final sfr'), None)
            col_final_bio = next((c for c in cols if c.strip().lower() == 'final biomasa'), None)
            
            # GF3, SGR, and SFR all use Final Biomasa as weight, so we use a single shared sum
            needs_weight = (col_gf3 or col_sgr or col_sfr) and col_final_bio
            
            # Helper function to generate the dynamic columns SQL
            # We must avoid nested window functions for DuckDB
            def get_dynamic_cols_only(partition_cols):
                sql_parts = []
                if col_fcr_eco and col_dif_bio and partition_cols:
                    sql_parts.append(f"""
                        "{col_dif_bio}" / NULLIF("Suma Biomasa Eco", 0) AS "factor_eco",
                        "{col_fcr_eco}" * ("{col_dif_bio}" / NULLIF("Suma Biomasa Eco", 0)) AS "Ponderación Eco",
                        SUM("{col_fcr_eco}" * ("{col_dif_bio}" / NULLIF("Suma Biomasa Eco", 0))) OVER (PARTITION BY {partition_cols}) AS "FCR Económico Acumulado"
                    """)
                if col_fcr_bio and col_dif_bio_mort and partition_cols:
                    sql_parts.append(f"""
                        "{col_dif_bio_mort}" / NULLIF("Suma Biomasa Bio", 0) AS "factor_bio",
                        "{col_fcr_bio}" * ("{col_dif_bio_mort}" / NULLIF("Suma Biomasa Bio", 0)) AS "Ponderación Bio",
                        SUM("{col_fcr_bio}" * ("{col_dif_bio_mort}" / NULLIF("Suma Biomasa Bio", 0))) OVER (PARTITION BY {partition_cols}) AS "FCR Biológico Acumulado"
                    """)
                if col_gf3 and col_final_bio and partition_cols:
                    sql_parts.append(f"""
                        "{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0) AS "factor_gf3",
                        "{col_gf3}" * ("{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0)) AS "Ponderación GF3",
                        SUM("{col_gf3}" * ("{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0))) OVER (PARTITION BY {partition_cols}) AS "GF3 Acumulado"
                    """)
                if col_sgr and col_final_bio and partition_cols:
                    sql_parts.append(f"""
                        "{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0) AS "factor_sgr",
                        "{col_sgr}" * ("{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0)) AS "Ponderación SGR",
                        SUM("{col_sgr}" * ("{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0))) OVER (PARTITION BY {partition_cols}) AS "SGR Acumulado"
                    """)
                if col_sfr and col_final_bio and partition_cols:
                    sql_parts.append(f"""
                        "{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0) AS "factor_sfr",
                        "{col_sfr}" * ("{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0)) AS "Ponderación SFR",
                        SUM("{col_sfr}" * ("{col_final_bio}" / NULLIF("Suma Biomasa Weight", 0))) OVER (PARTITION BY {partition_cols}) AS "SFR Acumulado"
                    """)
                if sql_parts:
                    return "," + ",".join(sql_parts)
                return ""
                
            def wrap_query_with_metrics(base_q, order_by_clause):
                if not col_date:
                    return f"{base_q} ORDER BY {order_by_clause}"
                if not (col_fcr_eco and col_dif_bio) and not (col_fcr_bio and col_dif_bio_mort) and not needs_weight:
                    return f"{base_q} ORDER BY {order_by_clause}"
                
                # If there's a batch column, we partition by Date AND Batch
                # so the accumulated metric applies per batch on that date.
                partition_cols = f'"{col_date}"'
                if col_lote:
                    partition_cols += f', "{col_lote}"'
                
                suma_eco_sql = f'SUM("{col_dif_bio}") OVER (PARTITION BY {partition_cols}) AS "Suma Biomasa Eco"' if col_dif_bio else 'NULL as "Suma Biomasa Eco"'
                suma_bio_sql = f'SUM("{col_dif_bio_mort}") OVER (PARTITION BY {partition_cols}) AS "Suma Biomasa Bio"' if col_dif_bio_mort else 'NULL as "Suma Biomasa Bio"'
                # Single shared weight column for GF3 and SGR
                suma_weight_sql = f'SUM("{col_final_bio}") OVER (PARTITION BY {partition_cols}) AS "Suma Biomasa Weight"' if needs_weight else 'NULL as "Suma Biomasa Weight"'

                # We wrap the base query to first calculate Sums per partition
                wrapped = f"""
                    WITH base_data AS (
                        {base_q}
                    ),
                    with_suma AS (
                        SELECT *,
                               {suma_eco_sql},
                               {suma_bio_sql},
                               {suma_weight_sql}
                        FROM base_data
                    )
                    SELECT * {get_dynamic_cols_only(partition_cols)}
                    FROM with_suma
                    ORDER BY {order_by_clause}
                """
                return wrapped



            # 3. Build Query
            # Priority 1: Average Units (New) - Groups all units/depts into one per batch
            if filters.get('avg_units'):
                group_keys = []
                if col_date: group_keys.append(f'"{col_date}"')
                if col_lote: group_keys.append(f'"{col_lote}"')
                if col_days: group_keys.append(f'"{col_days}"')
                # Note: We EXCLUDE col_dept to average across departments
                
                metric_selects = []
                desc = self.con.execute(f"DESCRIBE {table_name}").fetchall()
                numeric_types = ['DOUBLE', 'FLOAT', 'DECIMAL', 'BIGINT', 'INTEGER', 'INT', 'HUGEINT', 'SMALLINT', 'TINYINT']
                
                for col_info in desc:
                    col_name = col_info[0]
                    col_type = col_info[1].upper()
                    
                    if f'"{col_name}"' in group_keys: continue
                    
                    if any(t in col_type for t in numeric_types):
                        # For 'Average Unit', we average EVERYTHING (Biomass, Weight, FCR...)
                        metric_selects.append(f'AVG("{col_name}") as "{col_name}"')
                    else:
                        metric_selects.append(f'FIRST("{col_name}") as "{col_name}"')
                
                select_sql = ", ".join(group_keys + metric_selects)
                group_sql = ", ".join(group_keys)
                order_col = f'"{col_date}"' if col_date else '1'
                
                select_sql = ", ".join(group_keys + metric_selects)
                group_sql = ", ".join(group_keys)
                order_col = f'"{col_date}"' if col_date else '1'
                
                base_query = f"""
                    SELECT {select_sql}
                    FROM {table_name}
                    WHERE {where_sql}
                    GROUP BY {group_sql}
                """
                query = wrap_query_with_metrics(base_query, order_col)

            # Priority 2: Sum Units (Existing) - Groups by Dept, Sums Counts, Avgs Rates
            elif filters.get('sum_units'):
                group_keys = []
                if col_date: group_keys.append(f'"{col_date}"')
                if col_lote: group_keys.append(f'"{col_lote}"')
                if col_dept: group_keys.append(f'"{col_dept}"')
                if col_days: group_keys.append(f'"{col_days}"')
                
                metric_selects = []
                desc = self.con.execute(f"DESCRIBE {table_name}").fetchall()
                numeric_types = ['DOUBLE', 'FLOAT', 'DECIMAL', 'BIGINT', 'INTEGER', 'INT', 'HUGEINT', 'SMALLINT', 'TINYINT']
                
                for col_info in desc:
                    col_name = col_info[0]
                    col_type = col_info[1].upper()
                    
                    if f'"{col_name}"' in group_keys: continue
                    
                    if any(t in col_type for t in numeric_types):
                        is_rate = any(k in col_name.lower() for k in ['peso', 'weight', 'prom', 'avg', 'fcr', 'sfr', 'sgr', '%', 'factor', 'porcentaje', 'densidad'])
                        if is_rate:
                            metric_selects.append(f'AVG("{col_name}") as "{col_name}"')
                        else:
                            metric_selects.append(f'SUM("{col_name}") as "{col_name}"')
                    else:
                        metric_selects.append(f'FIRST("{col_name}") as "{col_name}"')
                
                select_sql = ", ".join(group_keys + metric_selects)
                group_sql = ", ".join(group_keys)
                order_col = f'"{col_date}"' if col_date else '1'
                
                base_query = f"""
                    SELECT {select_sql}
                    FROM {table_name}
                    WHERE {where_sql}
                    GROUP BY {group_sql}
                """
                query = wrap_query_with_metrics(base_query, order_col)

            # Priority 3: Raw Data
            else:
                order_col = f'"{col_date}"' if col_date else '1'
                
                base_query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {where_sql}
                """
                query = wrap_query_with_metrics(base_query, order_col)
            
            filtered_df = self.con.execute(query).df()
            
            # === GRANULARITY: Day vs Week ===
            # When 'Semana' is selected, group by week (Monday-Sunday, Chilean calendar)
            granularity = filters.get('granularity', 'Día')
            if granularity == 'Semana' and col_date:
                dates_parsed = pd.to_datetime(filtered_df[col_date], errors='coerce')
                # Monday of each week (weekday 0 = Monday)
                filtered_df['_group_date'] = dates_parsed - pd.to_timedelta(dates_parsed.dt.weekday, unit='D')
                group_date_col = '_group_date'
                
                # Sequential week number per batch: 1, 2, 3...
                if col_lote:
                    week_nums = filtered_df.groupby(col_lote)['_group_date'].transform(
                        lambda x: x.rank(method='dense').astype(int)
                    )
                else:
                    week_nums = filtered_df['_group_date'].rank(method='dense').astype(int)
                filtered_df['Semana'] = week_nums
                
                # Week date range for tooltips
                week_start = filtered_df['_group_date']
                week_end = week_start + pd.Timedelta(days=6)
                filtered_df['_week_start_str'] = pd.to_datetime(week_start).dt.strftime('%d-%m-%Y')
                filtered_df['_week_end_str'] = pd.to_datetime(week_end).dt.strftime('%d-%m-%Y')
                
                print(f"Weekly grouping: using _group_date + Semana column")
            else:
                group_date_col = col_date
            
            # === POST-PROCESSING: % Mortalidad Acumulada ===
            # Uses Hatchery 1 baseline + cumulative sum of per-period mortality
            try:
                col_mort_periodo = next((c for c in filtered_df.columns if c.strip() == 'Mortalidad, Número en el período'), None)
                
                if col_lote and col_date and col_mort_periodo:
                    dept_col_name = col_dept if col_dept else None
                    unit_col_name = next((c for c in cols if 'unidad' in c.lower() or 'unit' in c.lower() or 'jaula' in c.lower()), None)
                    numero_col_full = next((c for c in cols if c.strip().lower() == 'final número' or c.strip().lower() == 'final numero'), None)
                    
                    if dept_col_name and unit_col_name and numero_col_full:
                        # Step 1-2: Hatchery 1 baseline → "Cant inicial batch"
                        batch_filter_sql = ""
                        if filters.get('batches'):
                            batches_str = "', '".join([str(b) for b in filters['batches']])
                            batch_filter_sql = f""" AND "{col_lote}" IN ('{batches_str}')"""
                        
                        baseline_query = f"""
                            WITH hatchery_data AS (
                                SELECT "{col_lote}", "{unit_col_name}", "{col_date}", "{numero_col_full}"
                                FROM {table_name}
                                WHERE LOWER(TRIM("{dept_col_name}")) LIKE '%hatchery%1%'
                                  AND "{numero_col_full}" IS NOT NULL
                                  AND CAST("{numero_col_full}" AS DOUBLE) > 0
                                  {batch_filter_sql}
                            ),
                            earliest_per_unit AS (
                                SELECT "{col_lote}", "{unit_col_name}",
                                       FIRST("{numero_col_full}" ORDER BY "{col_date}" ASC) AS initial_numero
                                FROM hatchery_data
                                GROUP BY "{col_lote}", "{unit_col_name}"
                            )
                            SELECT "{col_lote}" AS batch, SUM(CAST(initial_numero AS DOUBLE)) AS cant_inicial
                            FROM earliest_per_unit
                            GROUP BY "{col_lote}"
                        """
                        baseline_df = self.con.execute(baseline_query).df()
                        cant_inicial_map = dict(zip(baseline_df['batch'], baseline_df['cant_inicial']))
                        
                        if cant_inicial_map:
                            # Write "Cant inicial batch" to all rows
                            filtered_df['Cant inicial batch'] = filtered_df[col_lote].map(cant_inicial_map)
                            
                            # Step 3: Cumulative sum of "Mortalidad, Número en el período" across dates per batch
                            # First, sum per date+batch
                            temp_mort = pd.to_numeric(filtered_df[col_mort_periodo], errors='coerce').fillna(0)
                            temp_batch = filtered_df[col_lote]
                            temp_grp_date = filtered_df[group_date_col]
                            
                            daily_mort_sum = temp_mort.groupby([temp_grp_date, temp_batch]).transform('sum')
                            
                            # Build a lookup: for each batch, sort dates and cumsum
                            # We need one cumulative value per (batch, date)
                            daily_agg = filtered_df[[col_lote, group_date_col]].copy()
                            daily_agg['_daily_mort'] = daily_mort_sum
                            daily_unique = daily_agg.drop_duplicates(subset=[col_lote, group_date_col]).sort_values([col_lote, group_date_col])
                            daily_unique['_mort_acum'] = daily_unique.groupby(col_lote)['_daily_mort'].cumsum()
                            
                            # Map cumulative values back to all rows
                            cumsum_map = daily_unique.set_index([col_lote, group_date_col])['_mort_acum']
                            filtered_df['Mortalidad Acumulada'] = filtered_df.set_index([col_lote, group_date_col]).index.map(cumsum_map).values
                            
                            # Step 4: % = Mortalidad Acumulada / Cant inicial batch
                            baseline_vals = filtered_df['Cant inicial batch']
                            filtered_df['% Mortalidad Acumulada'] = (
                                pd.to_numeric(filtered_df['Mortalidad Acumulada'], errors='coerce') 
                                / baseline_vals.replace(0, pd.NA)
                            ) * 100
                            
                            print(f"Successfully calculated '% Mortalidad Acumulada' (cumulative) with {len(cant_inicial_map)} batch baselines")
                            
                            # === % Pérdida Acumulada (reuses same baseline) ===
                            col_perd_periodo = next((c for c in filtered_df.columns if c.strip() == 'Pérdida total número en el período'), None)
                            if col_perd_periodo:
                                temp_perd = pd.to_numeric(filtered_df[col_perd_periodo], errors='coerce').fillna(0)
                                daily_perd_sum = temp_perd.groupby([temp_grp_date, temp_batch]).transform('sum')
                                
                                daily_perd_agg = filtered_df[[col_lote, group_date_col]].copy()
                                daily_perd_agg['_daily_perd'] = daily_perd_sum
                                perd_unique = daily_perd_agg.drop_duplicates(subset=[col_lote, group_date_col]).sort_values([col_lote, group_date_col])
                                perd_unique['_perd_acum'] = perd_unique.groupby(col_lote)['_daily_perd'].cumsum()
                                
                                perd_map = perd_unique.set_index([col_lote, group_date_col])['_perd_acum']
                                filtered_df['Pérdida Acumulada'] = filtered_df.set_index([col_lote, group_date_col]).index.map(perd_map).values
                                filtered_df['% Pérdida Acumulada'] = (
                                    pd.to_numeric(filtered_df['Pérdida Acumulada'], errors='coerce')
                                    / baseline_vals.replace(0, pd.NA)
                                ) * 100
                                print("Successfully calculated '% Pérdida Acumulada'")
                            
                            # === % Eliminación Acumulada (reuses same baseline) ===
                            col_elim_periodo = next((c for c in filtered_df.columns if c.strip() == 'Eliminados número en el período'), None)
                            if col_elim_periodo:
                                temp_elim = pd.to_numeric(filtered_df[col_elim_periodo], errors='coerce').fillna(0)
                                daily_elim_sum = temp_elim.groupby([temp_grp_date, temp_batch]).transform('sum')
                                
                                daily_elim_agg = filtered_df[[col_lote, group_date_col]].copy()
                                daily_elim_agg['_daily_elim'] = daily_elim_sum
                                elim_unique = daily_elim_agg.drop_duplicates(subset=[col_lote, group_date_col]).sort_values([col_lote, group_date_col])
                                elim_unique['_elim_acum'] = elim_unique.groupby(col_lote)['_daily_elim'].cumsum()
                                
                                elim_map = elim_unique.set_index([col_lote, group_date_col])['_elim_acum']
                                filtered_df['Eliminación Acumulada'] = filtered_df.set_index([col_lote, group_date_col]).index.map(elim_map).values
                                filtered_df['% Eliminación Acumulada'] = (
                                    pd.to_numeric(filtered_df['Eliminación Acumulada'], errors='coerce')
                                    / baseline_vals.replace(0, pd.NA)
                                ) * 100
                                print("Successfully calculated '% Eliminación Acumulada'")
                            
                            # === % Mortalidad por Causa Acumulada ===
                            # (Cumulative sum of cause / "Cant inicial batch") * 100 per batch
                            cause_columns = {
                                'Mortalidad, Número Embrionaria en el período': 'Embrionaria',
                                'Mortalidad, Número Deforme Embrionaria en el período': 'Deforme Embrionaria',
                                'Mortalidad, Número Micosis en el período': 'Micosis',
                                'Mortalidad, Número Daño Mecánico Otros en el período': 'Daño Mecánico Otros',
                                'Mortalidad, Número Desadaptado en el período': 'Desadaptado',
                                'Mortalidad, Número Deforme en el período': 'Deforme',
                                'Mortalidad, Número Descompuesto en el período': 'Descompuesto',
                                'Mortalidad, Número Aborto en el período': 'Aborto',
                                'Mortalidad, Número Daño Mecánico en el período': 'Daño Mecánico',
                                'Mortalidad, Número Sin causa Aparente  en el período': 'Sin causa Aparente',
                                'Mortalidad, Número Maduro en el período': 'Maduro',
                                'Mortalidad, Número Muestras en el período': 'Muestras',
                                'Mortalidad, Número Operculo Corto en el período': 'Operculo Corto',
                                'Mortalidad, Número Rezagado en el período': 'Rezagado',
                                'Mortalidad, Número Nefrocalcinosis en el período': 'Nefrocalcinosis',
                                'Mortalidad, Número Exofialosis en el período': 'Exofialosis',
                                'Mortalidad, Número Daño Mecánico por Muestreo en el período': 'Daño Mecánico por Muestreo',
                            }
                            
                            import unicodedata, re
                            def _norm(s):
                                s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
                                return re.sub(r'\s+', ' ', s.strip().lower())
                            
                            actual_col_map = {_norm(c): c for c in filtered_df.columns}
                            
                            cause_count_acum = 0
                            for exact_name, cause_display in cause_columns.items():
                                found_col = None
                                for c in filtered_df.columns:
                                    if c.strip() == exact_name:
                                        found_col = c
                                        break
                                if not found_col:
                                    norm_target = _norm(exact_name)
                                    found_col = actual_col_map.get(norm_target)
                                
                                if found_col:
                                    # 1. Sum by date+batch
                                    temp_cause = pd.to_numeric(filtered_df[found_col], errors='coerce').fillna(0)
                                    daily_cause_sum = temp_cause.groupby([temp_grp_date, temp_batch]).transform('sum')
                                    
                                    # 2. Cumulative sum by batch over time
                                    daily_cause_agg = filtered_df[[col_lote, group_date_col]].copy()
                                    daily_cause_agg['_daily_cause'] = daily_cause_sum
                                    cause_unique = daily_cause_agg.drop_duplicates(subset=[col_lote, group_date_col]).sort_values([col_lote, group_date_col])
                                    cause_unique['_cause_acum'] = cause_unique.groupby(col_lote)['_daily_cause'].cumsum()
                                    
                                    # 3. Map back and calculate %
                                    cause_map = cause_unique.set_index([col_lote, group_date_col])['_cause_acum']
                                    acum_val_col = f"Mortalidad {cause_display} Acumulada"
                                    filtered_df[acum_val_col] = filtered_df.set_index([col_lote, group_date_col]).index.map(cause_map).values
                                    
                                    output_name = f"% Mortalidad {cause_display} Acumulada"
                                    filtered_df[output_name] = (
                                        pd.to_numeric(filtered_df[acum_val_col], errors='coerce')
                                        / baseline_vals.replace(0, pd.NA)
                                    ) * 100
                                    cause_count_acum += 1
                                    
                            print(f"Successfully calculated {cause_count_acum} '% Mortalidad por Causa Acumulada' metrics")
            except Exception as e:
                print(f"Error calculating '% Mortalidad Acumulada': {e}")
                import traceback; traceback.print_exc()
            
            # === POST-PROCESSING: % Mortalidad diaria ===
            # (SUM "Mortalidad, Número en el período" / SUM "Final Número") * 100 per date+batch
            try:
                col_mort_periodo = next((c for c in filtered_df.columns if c.strip() == 'Mortalidad, Número en el período'), None)
                col_numero2 = next((c for c in filtered_df.columns if c.strip().lower() == 'final número' or c.strip().lower() == 'final numero'), None)
                
                if col_lote and col_date and col_mort_periodo and col_numero2:
                    temp_mort_d = pd.to_numeric(filtered_df[col_mort_periodo], errors='coerce').fillna(0)
                    temp_num_d = pd.to_numeric(filtered_df[col_numero2], errors='coerce').fillna(0)
                    temp_batch_d = filtered_df[col_lote]
                    temp_date_d = filtered_df[group_date_col]
                    
                    # Sum per-period mortality and number per date+batch (or week+batch)
                    sum_mort_daily = temp_mort_d.groupby([temp_date_d, temp_batch_d]).transform('sum')
                    sum_num_daily = temp_num_d.groupby([temp_date_d, temp_batch_d]).transform('sum')
                    
                    filtered_df['% Mortalidad diaria'] = (sum_mort_daily / sum_num_daily.replace(0, pd.NA)) * 100
                    print("Successfully calculated '% Mortalidad diaria'")
                    
            except Exception as e:
                print(f"Error calculating '% Mortalidad diaria': {e}")
                import traceback; traceback.print_exc()
            
            # === POST-PROCESSING: Pérdida diaria % ===
            # (SUM "Pérdida total número en el período" / SUM "Final Número") * 100 per date+batch
            try:
                col_perdida = next((c for c in filtered_df.columns if c.strip() == 'Pérdida total número en el período'), None)
                col_numero_p = next((c for c in filtered_df.columns if c.strip().lower() == 'final número' or c.strip().lower() == 'final numero'), None)
                
                if col_lote and col_date and col_perdida and col_numero_p:
                    temp_perd = pd.to_numeric(filtered_df[col_perdida], errors='coerce').fillna(0)
                    temp_num_p = pd.to_numeric(filtered_df[col_numero_p], errors='coerce').fillna(0)
                    temp_batch_p = filtered_df[col_lote]
                    temp_date_p = filtered_df[group_date_col]
                    
                    sum_perd = temp_perd.groupby([temp_date_p, temp_batch_p]).transform('sum')
                    sum_num_p = temp_num_p.groupby([temp_date_p, temp_batch_p]).transform('sum')
                    
                    filtered_df['Pérdida diaria %'] = (sum_perd / sum_num_p.replace(0, pd.NA)) * 100
                    print("Successfully calculated 'Pérdida diaria %'")
                    
            except Exception as e:
                print(f"Error calculating 'Pérdida diaria %': {e}")
                import traceback; traceback.print_exc()
            
            # === POST-PROCESSING: Eliminación diaria % ===
            # (SUM "Eliminados número en el período" / SUM "Final Número") * 100 per date+batch
            try:
                col_elim = next((c for c in filtered_df.columns if c.strip() == 'Eliminados número en el período'), None)
                col_numero_e = next((c for c in filtered_df.columns if c.strip().lower() == 'final número' or c.strip().lower() == 'final numero'), None)
                
                if col_lote and col_date and col_elim and col_numero_e:
                    temp_elim = pd.to_numeric(filtered_df[col_elim], errors='coerce').fillna(0)
                    temp_num_e = pd.to_numeric(filtered_df[col_numero_e], errors='coerce').fillna(0)
                    temp_batch_e = filtered_df[col_lote]
                    temp_date_e = filtered_df[group_date_col]
                    
                    sum_elim = temp_elim.groupby([temp_date_e, temp_batch_e]).transform('sum')
                    sum_num_e = temp_num_e.groupby([temp_date_e, temp_batch_e]).transform('sum')
                    
                    filtered_df['Eliminación diaria %'] = (sum_elim / sum_num_e.replace(0, pd.NA)) * 100
                    print("Successfully calculated 'Eliminación diaria %'")
                    
            except Exception as e:
                print(f"Error calculating 'Eliminación diaria %': {e}")
                import traceback; traceback.print_exc()
            
            # === POST-PROCESSING: % Mortalidad por Causa Diaria ===
            # For each cause: (SUM cause per batch+date) / (SUM "Final Número" per batch+date) * 100
            try:
                col_final_numero = next((c for c in filtered_df.columns if c.strip() == 'Final Número'), None)
                
                # Debug: list all mortality-related columns
                mort_cols_debug = [c for c in filtered_df.columns if 'ortalidad' in c and 'mero' in c.lower()]
                print(f"[cause-debug] Mortality columns found ({len(mort_cols_debug)}): {mort_cols_debug}")
                print(f"[cause-debug] col_final_numero={col_final_numero}, col_lote={col_lote}, col_date={col_date}")
                
                if col_lote and col_date and col_final_numero:
                    # EXACT column names as provided by the user → display cause name
                    cause_columns = {
                        'Mortalidad, Número Embrionaria en el período': 'Embrionaria',
                        'Mortalidad, Número Deforme Embrionaria en el período': 'Deforme Embrionaria',
                        'Mortalidad, Número Micosis en el período': 'Micosis',
                        'Mortalidad, Número Daño Mecánico Otros en el período': 'Daño Mecánico Otros',
                        'Mortalidad, Número Desadaptado en el período': 'Desadaptado',
                        'Mortalidad, Número Deforme en el período': 'Deforme',
                        'Mortalidad, Número Descompuesto en el período': 'Descompuesto',
                        'Mortalidad, Número Aborto en el período': 'Aborto',
                        'Mortalidad, Número Daño Mecánico en el período': 'Daño Mecánico',
                        'Mortalidad, Número Sin causa Aparente  en el período': 'Sin causa Aparente',
                        'Mortalidad, Número Maduro en el período': 'Maduro',
                        'Mortalidad, Número Muestras en el período': 'Muestras',
                        'Mortalidad, Número Operculo Corto en el período': 'Operculo Corto',
                        'Mortalidad, Número Rezagado en el período': 'Rezagado',
                        'Mortalidad, Número Nefrocalcinosis en el período': 'Nefrocalcinosis',
                        'Mortalidad, Número Exofialosis en el período': 'Exofialosis',
                        'Mortalidad, Número Daño Mecánico por Muestreo en el período': 'Daño Mecánico por Muestreo',
                    }
                    
                    # Pre-compute "Final Número" sum per batch+date
                    temp_final_numero = pd.to_numeric(filtered_df[col_final_numero], errors='coerce').fillna(0)
                    temp_batch_mc = filtered_df[col_lote]
                    temp_date_mc = filtered_df[group_date_col]
                    sum_final_numero = temp_final_numero.groupby([temp_date_mc, temp_batch_mc]).transform('sum')
                    
                    # Build normalized lookup: normalize(col) -> actual col name
                    import unicodedata, re
                    def _norm(s):
                        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
                        return re.sub(r'\s+', ' ', s.strip().lower())
                    
                    actual_col_map = {_norm(c): c for c in filtered_df.columns}
                    
                    cause_count = 0
                    for exact_name, cause_display in cause_columns.items():
                        # Try exact match first
                        found_col = None
                        for c in filtered_df.columns:
                            if c.strip() == exact_name:
                                found_col = c
                                break
                        
                        # Fallback: normalized match
                        if not found_col:
                            norm_target = _norm(exact_name)
                            found_col = actual_col_map.get(norm_target)
                        
                        if found_col:
                            temp_cause = pd.to_numeric(filtered_df[found_col], errors='coerce').fillna(0)
                            sum_cause = temp_cause.groupby([temp_date_mc, temp_batch_mc]).transform('sum')
                            
                            output_name = f"% Mortalidad {cause_display} Diaria"
                            filtered_df[output_name] = (sum_cause / sum_final_numero.replace(0, pd.NA)) * 100
                            cause_count += 1
                        else:
                            print(f"  [cause] Column NOT found: '{exact_name}'")
                    
                    print(f"Successfully calculated {cause_count}/17 '% Mortalidad por Causa Diaria' metrics")
                else:
                    print(f"[cause] Skipped: col_lote={col_lote}, col_date={col_date}, col_final_numero={col_final_numero}")
                    
            except Exception as e:
                print(f"Error calculating '% Mortalidad por Causa Diaria': {e}")
                import traceback; traceback.print_exc()
            
            # === POST-PROCESSING: Peso promedio ===
            # Weighted average of "Final Peso prom" using "Final Número" as weight, per date+batch
            try:
                col_peso_prom = next((c for c in filtered_df.columns if c.strip().lower() == 'final peso prom'), None)
                col_numero_pp = next((c for c in filtered_df.columns if c.strip().lower() == 'final número' or c.strip().lower() == 'final numero'), None)
                
                if col_lote and col_date and col_peso_prom and col_numero_pp:
                    peso_vals = pd.to_numeric(filtered_df[col_peso_prom], errors='coerce').fillna(0)
                    num_vals = pd.to_numeric(filtered_df[col_numero_pp], errors='coerce').fillna(0)
                    temp_batch_pp = filtered_df[col_lote]
                    temp_date_pp = filtered_df[group_date_col]
                    
                    # Step 1: Sum "Final Número" per date+batch (or week+batch)
                    sum_num_pp = num_vals.groupby([temp_date_pp, temp_batch_pp]).transform('sum')
                    
                    # Step 2-3: factor = each row's "Final Número" / sum
                    factor_pp = num_vals / sum_num_pp.replace(0, pd.NA)
                    
                    # Step 4: weighted = factor * "Final Peso prom"
                    weighted_pp = factor_pp * peso_vals
                    
                    # Step 5: Sum weighted values per date+batch = "Peso promedio"
                    filtered_df['Peso promedio'] = weighted_pp.groupby([temp_date_pp, temp_batch_pp]).transform('sum')
                    print("Successfully calculated 'Peso promedio'")
                    
            except Exception as e:
                print(f"Error calculating 'Peso promedio': {e}")
                import traceback; traceback.print_exc()
            
            return filtered_df
            
        except Exception as e:
            print(f"Query Error: {e}")
            import traceback; traceback.print_exc()
            return pd.DataFrame()

    # ====================================================================
    # KPIs y Proyecciones por Batch
    # ====================================================================

    def ingest_kpis_proyecciones(self, file):
        """
        Reads the 'KPIs y Proyecciones por Batch' Excel file.
        - 'KPIs' sheet → table 'kpi_thresholds'
        - 'Batch XX' sheets → table 'proyecciones_data'
        """
        try:
            sheets_dict = pd.read_excel(file, sheet_name=None)
        except Exception:
            file.seek(0)
            sheets_dict = pd.read_excel(file, sheet_name=None)

        changed = False

        # --- 1. KPIs Sheet ---
        if 'KPIs' in sheets_dict:
            kpi_df = sheets_dict['KPIs'].copy()
            # Normalize column names
            kpi_df.columns = [str(c).strip() for c in kpi_df.columns]
            
            # Ensure expected columns exist
            col_tipo = next((c for c in kpi_df.columns if 'tipo' in c.lower()), None)
            col_dept = next((c for c in kpi_df.columns if 'departamento' in c.lower() or 'dept' in c.lower()), None)
            col_menor = next((c for c in kpi_df.columns if 'menor' in c.lower()), None)

            if col_tipo and col_dept and col_menor:
                clean_kpi = pd.DataFrame({
                    'tipo_kpi': kpi_df[col_tipo].astype(str).str.strip(),
                    'departamento': kpi_df[col_dept].astype(str).str.strip(),
                    'menor_a': pd.to_numeric(kpi_df[col_menor], errors='coerce'),
                })
                clean_kpi = clean_kpi.dropna(subset=['menor_a'])

                try:
                    self.con.register('_tmp_kpi', clean_kpi)
                    self.con.execute("CREATE OR REPLACE TABLE kpi_thresholds AS SELECT * FROM _tmp_kpi")
                    self.con.unregister('_tmp_kpi')
                    changed = True
                    print(f"Ingested {len(clean_kpi)} KPI thresholds")
                except Exception as e:
                    print(f"Error ingesting KPI thresholds: {e}")
            else:
                print(f"KPIs sheet: expected columns not found. Got: {kpi_df.columns.tolist()}")

        # --- 2. Batch Sheets (Proyecciones) ---
        all_proj = []
        for sheet_name, df in sheets_dict.items():
            if not sheet_name.lower().startswith('batch'):
                continue

            # Extract batch number/name from sheet name: "Batch 65" → "65"
            batch_id = sheet_name.replace('Batch', '').replace('batch', '').strip()
            
            df = df.copy()
            df.columns = [str(c).strip() for c in df.columns]

            # Resolve FECHA column
            col_fecha = next((c for c in df.columns if 'fecha' in c.lower() or 'date' in c.lower()), None)
            if col_fecha:
                df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce')

            # Force all numeric columns to float
            for col in df.columns:
                if col == col_fecha:
                    continue
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

            df['batch'] = batch_id
            all_proj.append(df)

        if all_proj:
            proj_df = pd.concat(all_proj, ignore_index=True)
            try:
                self.con.register('_tmp_proj', proj_df)
                self.con.execute("CREATE OR REPLACE TABLE proyecciones_data AS SELECT * FROM _tmp_proj")
                self.con.unregister('_tmp_proj')
                changed = True
                print(f"Ingested {len(proj_df)} projection rows across {len(all_proj)} batches")
            except Exception as e:
                print(f"Error ingesting projections: {e}")

        if changed:
            self._bump_revision()

    def get_kpi_thresholds(self):
        """
        Returns KPI thresholds as a nested dict: {tipo_kpi: {departamento: valor}}.
        """
        try:
            tables = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            if 'kpi_thresholds' not in tables:
                return {}

            df = self.con.execute("SELECT tipo_kpi, departamento, menor_a FROM kpi_thresholds").df()
            result = {}
            for _, row in df.iterrows():
                tipo = row['tipo_kpi']
                dept = row['departamento']
                val = row['menor_a']
                if tipo not in result:
                    result[tipo] = {}
                result[tipo][dept] = val
            return result
        except Exception as e:
            print(f"Error getting KPI thresholds: {e}")
            return {}

    def get_proyecciones_metadata(self):
        """
        Returns metadata for projections: {'batches': [...], 'variables': [...]}.
        """
        try:
            tables = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            if 'proyecciones_data' not in tables:
                return {}

            batches = [r[0] for r in self.con.execute(
                "SELECT DISTINCT batch FROM proyecciones_data ORDER BY batch"
            ).fetchall()]

            cols = [c[0] for c in self.con.execute("DESCRIBE proyecciones_data").fetchall()]
            # Exclude structural columns
            exclude = ['batch', 'fecha', 'date']
            variables = [c for c in cols if c.lower() not in exclude]

            return {'batches': batches, 'variables': variables}
        except Exception as e:
            print(f"Error getting projections metadata: {e}")
            return {}

    def get_proyecciones_data(self, batches=None, variables=None, date_range=None):
        """
        Returns projection DataFrame filtered by batch, variables, and optional date range.
        """
        try:
            tables = [t[0] for t in self.con.execute("SHOW TABLES").fetchall()]
            if 'proyecciones_data' not in tables:
                return pd.DataFrame()

            cols = [c[0] for c in self.con.execute("DESCRIBE proyecciones_data").fetchall()]
            
            # Resolve fecha column
            col_fecha = next((c for c in cols if 'fecha' in c.lower() or 'date' in c.lower()), None)

            where_parts = []

            if batches:
                # Projection batches are numeric-only (e.g. '65'),
                # but production batches may have suffixes (e.g. '65SJ').
                # Extract leading digits for fuzzy matching.
                import re
                proj_batch_ids = set(
                    r[0] for r in self.con.execute(
                        "SELECT DISTINCT batch FROM proyecciones_data"
                    ).fetchall()
                )
                matched_ids = set()
                for b in batches:
                    b_str = str(b).strip()
                    # Direct match first
                    if b_str in proj_batch_ids:
                        matched_ids.add(b_str)
                    else:
                        # Extract leading digits (e.g. '65SJ' → '65')
                        m = re.match(r'(\d+)', b_str)
                        if m and m.group(1) in proj_batch_ids:
                            matched_ids.add(m.group(1))

                if matched_ids:
                    ids_str = "', '".join(matched_ids)
                    where_parts.append(f"batch IN ('{ids_str}')")
                else:
                    return pd.DataFrame()  # No matching batches

            if date_range and col_fecha and len(date_range) == 2:
                start = pd.to_datetime(date_range[0]).strftime('%Y-%m-%d')
                end = pd.to_datetime(date_range[1]).strftime('%Y-%m-%d')
                where_parts.append(f'"{col_fecha}" BETWEEN \'{start}\' AND \'{end}\'')

            where_sql = " AND ".join(where_parts) if where_parts else "1=1"

            # Select only requested variables + batch + fecha
            select_cols = ['"batch"']
            if col_fecha:
                select_cols.append(f'"{col_fecha}"')

            if variables:
                for var in variables:
                    matched = next((c for c in cols if c == var or c.lower() == var.lower()), None)
                    if matched:
                        select_cols.append(f'"{matched}"')
            else:
                # Select all non-structural columns
                exclude_proj = ['batch', col_fecha.lower() if col_fecha else '']
                for c in cols:
                    if c.lower() not in exclude_proj:
                        select_cols.append(f'"{c}"')

            if len(select_cols) <= 2:
                # No valid variables matched
                return pd.DataFrame()

            select_sql = ", ".join(select_cols)
            order_col = f'"{col_fecha}"' if col_fecha else 'batch'
            query = f"SELECT {select_sql} FROM proyecciones_data WHERE {where_sql} ORDER BY batch, {order_col}"

            return self.con.execute(query).df()
        except Exception as e:
            print(f"Error getting projections data: {e}")
            import traceback; traceback.print_exc()
            return pd.DataFrame()
