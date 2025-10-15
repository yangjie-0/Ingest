-- 1. record_error 表（行エラーテーブル）
CREATE TABLE record_error (
    batch_id TEXT NOT NULL,
    step TEXT NOT NULL,
    record_ref TEXT,
    error_cd TEXT,
    error_detail TEXT,
    raw_fragment TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id)
);

COMMENT ON TABLE record_error IS '行エラーテーブル';
COMMENT ON COLUMN record_error.batch_id IS 'バッチID';
COMMENT ON COLUMN record_error.step IS '処理ステップ: INGEST:取込、CLEANSE:クレンジング、UPSERT:アップサート';
COMMENT ON COLUMN record_error.record_ref IS 'レコード参照キー: TEMP行IDやファイル行内番号"line:123""temp_row_id=3456"';
COMMENT ON COLUMN record_error.error_cd IS 'エラーコード: PARSE_FAILED:CSV行パース失敗、MISSING_COLUMN:必須行がない、INVALID_ENCODING:文字コード不正、ROW_TOO_LARGE:行サイズ超過など';
COMMENT ON COLUMN record_error.error_detail IS 'エラー処理内容: 人間に説明可能な説明（CSV行パース失敗しました）';
COMMENT ON COLUMN record_error.raw_fragment IS '元データ断片: エラーを起こしたCSV行やJSON文字列抜粋';
COMMENT ON COLUMN record_error.cre_at IS '登録日時';
COMMENT ON COLUMN record_error.upd_at IS '更新日時';

-- 2. m_data_import_setting 表（ファイル取り込みルールマスタ）
CREATE TABLE m_data_import_setting (
    profile_id BIGINT NOT NULL,
    usage_nm TEXT NOT NULL,
    group_company_cd TEXT NOT NULL,
    target_entity TEXT NOT NULL,
    character_cd TEXT DEFAULT 'UTF-8',
    delimiter TEXT DEFAULT ',',
    header_row_index INT DEFAULT 1,
    skip_row_count INT DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    import_setting_remarks TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id),
    UNIQUE (usage_nm)
);

COMMENT ON TABLE m_data_import_setting IS 'ファイル取り込みルールマスタ: GP会社ごとのファイル取込ルールヘッダ';
COMMENT ON COLUMN m_data_import_setting.profile_id IS 'プロファイルID';
COMMENT ON COLUMN m_data_import_setting.usage_nm IS '用途名: 任意、自由テキスト（KM-PRODUCT、RKE-STOCKなど）';
COMMENT ON COLUMN m_data_import_setting.group_company_cd IS 'GP会社コード';
COMMENT ON COLUMN m_data_import_setting.target_entity IS '処理モード: PRODUCT:商品、EVENT:イベント、PRODUCT_EAV:商品EAV ファイル基本方針';
COMMENT ON COLUMN m_data_import_setting.character_cd IS '文字コード';
COMMENT ON COLUMN m_data_import_setting.delimiter IS '区切り文字';
COMMENT ON COLUMN m_data_import_setting.header_row_index IS 'ヘッダ行番号';
COMMENT ON COLUMN m_data_import_setting.skip_row_count IS 'スキップ行数';
COMMENT ON COLUMN m_data_import_setting.is_active IS '有効フラグ';
COMMENT ON COLUMN m_data_import_setting.import_setting_remarks IS '備考';
COMMENT ON COLUMN m_data_import_setting.cre_at IS '登録日時';
COMMENT ON COLUMN m_data_import_setting.upd_at IS '更新日時';

-- 3. m_fixed_to_attr_map 表（属性ルール投影マスタ）
CREATE TABLE m_fixed_to_attr_map (
    map_id BIGINT NOT NULL,
    group_company_cd TEXT NOT NULL,
    data_kind TEXT NOT NULL,
    attr_cd TEXT NOT NULL,
    source_id_column TEXT,
    source_label_column TEXT,
    value_role TEXT,
    data_type_override TEXT NOT NULL,
    split_mode TEXT,
    is_active BOOLEAN,
    priority INT DEFAULT 100,
    fixed_remarks TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (map_id)
);

COMMENT ON TABLE m_fixed_to_attr_map IS '属性ルール投影マスタ';
COMMENT ON COLUMN m_fixed_to_attr_map.map_id IS 'マップID';
COMMENT ON COLUMN m_fixed_to_attr_map.group_company_cd IS 'GP会社コード';
COMMENT ON COLUMN m_fixed_to_attr_map.data_kind IS 'データ種別: PRODUCT:商品、PRODUCT_MNG:製品、EVENT:在庫、販売';
COMMENT ON COLUMN m_fixed_to_attr_map.attr_cd IS '項目コード';
COMMENT ON COLUMN m_fixed_to_attr_map.source_id_column IS 'TEMPID列名';
COMMENT ON COLUMN m_fixed_to_attr_map.source_label_column IS 'TEMP名称列名';
COMMENT ON COLUMN m_fixed_to_attr_map.value_role IS '値の役割: ID_ONLY:IDのみ、LABEL_ONLY:ラベルのみ、ID_AND_LABEL:IDラベル両方';
COMMENT ON COLUMN m_fixed_to_attr_map.data_type_override IS 'データタイプ上書き: TEXT:テキスト、NUM:数値、DATE:日付、LIST:リスト、BOOL:真偽、REF:外部参照';
COMMENT ON COLUMN m_fixed_to_attr_map.split_mode IS '区切り文字';
COMMENT ON COLUMN m_fixed_to_attr_map.is_active IS '有効フラグ';
COMMENT ON COLUMN m_fixed_to_attr_map.priority IS '優先度';
COMMENT ON COLUMN m_fixed_to_attr_map.fixed_remarks IS '備考';
COMMENT ON COLUMN m_fixed_to_attr_map.cre_at IS '登録日時';
COMMENT ON COLUMN m_fixed_to_attr_map.upd_at IS '更新日時';

-- 4. m_data_import_d 表（ファイル取込ルール詳細マスタ）
CREATE TABLE m_data_import_d (
    profile_id BIGINT NOT NULL,
    column_seq INT NOT NULL,
    target_entity TEXT NOT NULL,
    attr_cd TEXT,
    target_column TEXT,
    cast_type TEXT,
    transform_expr TEXT,
    is_required BOOLEAN DEFAULT FALSE,
    import_remarks TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, column_seq, target_entity)
);

COMMENT ON TABLE m_data_import_d IS 'ファイル取込ルール詳細マスタ: ファイル取込ルール詳細';
COMMENT ON COLUMN m_data_import_d.profile_id IS 'プロファイルID';
COMMENT ON COLUMN m_data_import_d.column_seq IS '列番号: CSV上の列インデックス1始まり';
COMMENT ON COLUMN m_data_import_d.target_entity IS 'ターゲット: PRODUCT:商品、EVENT:イベント、PRODUCT_EAV:商品EAV　ルールの行ごとの上書き';
COMMENT ON COLUMN m_data_import_d.attr_cd IS '項目コード';
COMMENT ON COLUMN m_data_import_d.target_column IS '固定カラム: 直差し先を明示したい場合（固定カラム）';
COMMENT ON COLUMN m_data_import_d.cast_type IS '型変換タイプ: TEXT:テキスト、NUM:数値、DATE:日付、BOOL:真偽';
COMMENT ON COLUMN m_data_import_d.transform_expr IS '値変換式: 例: trim(@), upper(@), to_timestamp(@,''YYYY-MM-DD'')';
COMMENT ON COLUMN m_data_import_d.is_required IS '必須フラグ';
COMMENT ON COLUMN m_data_import_d.import_remarks IS '備考';
COMMENT ON COLUMN m_data_import_d.cre_at IS '登録日時';
COMMENT ON COLUMN m_data_import_d.upd_at IS '更新日時';

-- 5. temp_product_parsed 表（取込商品テーブル）
CREATE TABLE temp_product_parsed (
    temp_row_id UUID NOT NULL DEFAULT gen_random_uuid(),
    batch_id TEXT NOT NULL,
    line_no BIGINT NOT NULL,
    source_group_company_cd TEXT NOT NULL,
    source_product_cd TEXT,
    source_product_management_cd TEXT,
    source_brand_id TEXT,
    source_brand_nm TEXT,
    source_category_1_id TEXT,
    source_category_1_nm TEXT,
    source_category_2_id TEXT,
    source_category_2_nm TEXT,
    source_category_3_id TEXT,
    source_category_3_nm TEXT,
    source_product_status_cd TEXT,
    source_product_status_nm TEXT,
    source_new_used_kbn TEXT,
    source_quantity TEXT,
    source_stock_existence_cd TEXT,
    source_stock_existence_nm TEXT,
    source_sale_permission_cd TEXT,
    source_sale_permission_nm TEXT,
    source_transfer_status TEXT,
    source_repair_status TEXT,
    source_reservation_status TEXT,
    source_consignment_status TEXT,
    source_accept_status TEXT,
    source_ec_listing_kbn TEXT,
    source_assessment_price_excl_tax TEXT,
    source_assessment_price_incl_tax TEXT,
    source_assessment_tax_rate TEXT,
    source_purchase_price_excl_tax TEXT,
    source_purchase_price_incl_tax TEXT,
    source_purchase_tax_rate TEXT,
    source_display_price_excl_tax TEXT,
    source_display_price_incl_tax TEXT,
    source_display_tax_rate TEXT,
    source_sales_price_excl_tax TEXT,
    source_sales_price_incl_tax TEXT,
    source_sales_tax_rate TEXT,
    source_purchase_rank TEXT,
    source_purchase_rank_name TEXT,
    source_sales_rank TEXT,
    source_sales_rank_name TEXT,
    source_sales_channel_nm TEXT,
    source_sales_channel_region TEXT,
    source_sales_channel_method TEXT,
    source_sales_channel_target TEXT,
    source_purchase_channel_nm TEXT,
    source_purchase_channel_region TEXT,
    source_purchase_channel_method TEXT,
    source_purchase_channel_target TEXT,
    source_store_id TEXT,
    source_store_nm TEXT,
    source_consignor_group_company_id TEXT,
    source_consignor_product_cd TEXT,
    extras_json JSONB NOT NULL DEFAULT '{}',
    step_status TEXT NOT NULL DEFAULT 'ready',
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (temp_row_id)
);

COMMENT ON TABLE temp_product_parsed IS '取込商品テーブル';
COMMENT ON COLUMN temp_product_parsed.temp_row_id IS '取込データ行ID';
COMMENT ON COLUMN temp_product_parsed.batch_id IS 'バッチID';
COMMENT ON COLUMN temp_product_parsed.line_no IS '行番号: CSV行番号';
COMMENT ON COLUMN temp_product_parsed.source_group_company_cd IS 'GP会社コード: KM,RKE,KBO';
COMMENT ON COLUMN temp_product_parsed.source_product_cd IS '連携元商品コード';
COMMENT ON COLUMN temp_product_parsed.source_product_management_cd IS '製品管理コード';
COMMENT ON COLUMN temp_product_parsed.source_brand_id IS '連携元ブランドID';
COMMENT ON COLUMN temp_product_parsed.source_brand_nm IS '連携元ブランド名';
COMMENT ON COLUMN temp_product_parsed.source_category_1_id IS '連携元カテゴリ1ID';
COMMENT ON COLUMN temp_product_parsed.source_category_1_nm IS '連携元カテゴリ1名';
COMMENT ON COLUMN temp_product_parsed.source_category_2_id IS '連携元カテゴリ2ID';
COMMENT ON COLUMN temp_product_parsed.source_category_2_nm IS '連携元カテゴリ2名';
COMMENT ON COLUMN temp_product_parsed.source_category_3_id IS '連携元カテゴリ3ID';
COMMENT ON COLUMN temp_product_parsed.source_category_3_nm IS '連携元カテゴリ3名';
COMMENT ON COLUMN temp_product_parsed.source_product_status_cd IS '連携元商品状態コード';
COMMENT ON COLUMN temp_product_parsed.source_product_status_nm IS '連携元商品状態名';
COMMENT ON COLUMN temp_product_parsed.source_new_used_kbn IS '連携元新品区分';
COMMENT ON COLUMN temp_product_parsed.source_quantity IS '連携元新品数';
COMMENT ON COLUMN temp_product_parsed.source_stock_existence_cd IS '連携元在庫有無コード';
COMMENT ON COLUMN temp_product_parsed.source_stock_existence_nm IS '連携元在庫有無名';
COMMENT ON COLUMN temp_product_parsed.source_sale_permission_cd IS '連携元販売可否コード';
COMMENT ON COLUMN temp_product_parsed.source_sale_permission_nm IS '連携元販売可否名';
COMMENT ON COLUMN temp_product_parsed.source_transfer_status IS '連携元移動状態';
COMMENT ON COLUMN temp_product_parsed.source_repair_status IS '連携元修理状態';
COMMENT ON COLUMN temp_product_parsed.source_reservation_status IS '連携元予約状態';
COMMENT ON COLUMN temp_product_parsed.source_consignment_status IS '連携元委託状態';
COMMENT ON COLUMN temp_product_parsed.source_accept_status IS '連携元受託状態';
COMMENT ON COLUMN temp_product_parsed.source_ec_listing_kbn IS '連携元EC掲載区分';
COMMENT ON COLUMN temp_product_parsed.source_assessment_price_excl_tax IS '連携元査定価格(税抜き): STEP1未使用';
COMMENT ON COLUMN temp_product_parsed.source_assessment_price_incl_tax IS '連携元査定価格(税込み): STEP1未使用';
COMMENT ON COLUMN temp_product_parsed.source_assessment_tax_rate IS '連携元税率: STEP1未使用';
COMMENT ON COLUMN temp_product_parsed.source_purchase_price_excl_tax IS '連携元買取価格(税抜き)';
COMMENT ON COLUMN temp_product_parsed.source_purchase_price_incl_tax IS '連携元買取価格(税込み)';
COMMENT ON COLUMN temp_product_parsed.source_purchase_tax_rate IS '連携元税率';
COMMENT ON COLUMN temp_product_parsed.source_display_price_excl_tax IS '連携元表示価格(税抜き)';
COMMENT ON COLUMN temp_product_parsed.source_display_price_incl_tax IS '連携元表示価格(税込み)';
COMMENT ON COLUMN temp_product_parsed.source_display_tax_rate IS '連携元税率';
COMMENT ON COLUMN temp_product_parsed.source_sales_price_excl_tax IS '連携元売上価格(税抜き)';
COMMENT ON COLUMN temp_product_parsed.source_sales_price_incl_tax IS '連携元売上価格(税込み)';
COMMENT ON COLUMN temp_product_parsed.source_sales_tax_rate IS '連携元税率';
COMMENT ON COLUMN temp_product_parsed.source_purchase_rank IS '連携元仕入ランク';
COMMENT ON COLUMN temp_product_parsed.source_purchase_rank_name IS '連携元仕入ランク名';
COMMENT ON COLUMN temp_product_parsed.source_sales_rank IS '連携元販売ランク';
COMMENT ON COLUMN temp_product_parsed.source_sales_rank_name IS '連携元販売ランク名';
COMMENT ON COLUMN temp_product_parsed.source_sales_channel_nm IS '連携元販売チャネル名';
COMMENT ON COLUMN temp_product_parsed.source_sales_channel_region IS '連携元販売チャネル地域';
COMMENT ON COLUMN temp_product_parsed.source_sales_channel_method IS '連携元販売チャネル方法';
COMMENT ON COLUMN temp_product_parsed.source_sales_channel_target IS '連携元販売チャネル対象';
COMMENT ON COLUMN temp_product_parsed.source_purchase_channel_nm IS '連携元仕入チャネル名';
COMMENT ON COLUMN temp_product_parsed.source_purchase_channel_region IS '連携元仕入チャネル地域';
COMMENT ON COLUMN temp_product_parsed.source_purchase_channel_method IS '連携元仕入チャネル方法';
COMMENT ON COLUMN temp_product_parsed.source_purchase_channel_target IS '連携元仕入チャネル対象';
COMMENT ON COLUMN temp_product_parsed.source_store_id IS '連携元の連携元店舗ID';
COMMENT ON COLUMN temp_product_parsed.source_store_nm IS '連携元の連携元店舗名';
COMMENT ON COLUMN temp_product_parsed.source_consignor_group_company_id IS '連携元委託元GP会社ID';
COMMENT ON COLUMN temp_product_parsed.source_consignor_product_cd IS '連携元委託元商品コード';
COMMENT ON COLUMN temp_product_parsed.extras_json IS '連携元商品、製品項目: 未定義項目';
COMMENT ON COLUMN temp_product_parsed.step_status IS '取込状態: READY:未処理、PICKED:処理中、DONE:処理完了、ERROR_RETRY:一時エラー、ERROR_FATAL:永続エラー';
COMMENT ON COLUMN temp_product_parsed.cre_at IS '登録日時';
COMMENT ON COLUMN temp_product_parsed.upd_at IS '更新日時';

-- 6. cl_product_attr 表（クレンジング結果商品属性テーブル）
CREATE TABLE cl_product_attr (
    batch_id TEXT NOT NULL,
    temp_row_id UUID NOT NULL,
    attr_cd TEXT NOT NULL,
    attr_seq SMALLINT NOT NULL,
    source_id TEXT,
    source_label TEXT,
    source_raw TEXT,
    value_text TEXT,
    value_num NUMERIC(18,4),
    value_date DATE,
    value_cd TEXT,
    g_list_item_id BIGINT,
    data_type TEXT,
    quality_flag TEXT,
    quality_detail_json JSONB,
    provenance_json JSONB,
    rule_version TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id, temp_row_id, attr_cd, attr_seq)
);

COMMENT ON TABLE cl_product_attr IS 'クレンジング結果商品属性テーブル';
COMMENT ON COLUMN cl_product_attr.batch_id IS 'バッチID';
COMMENT ON COLUMN cl_product_attr.temp_row_id IS '取込行番号（tempの行）';
COMMENT ON COLUMN cl_product_attr.attr_cd IS '項目コード: 例）BRAND, DIAL_COLOR';
COMMENT ON COLUMN cl_product_attr.attr_seq IS '順序: 同一属性内の順序（マルチ属性用）';
COMMENT ON COLUMN cl_product_attr.source_id IS '連携元ID';
COMMENT ON COLUMN cl_product_attr.source_label IS '連携元名';
COMMENT ON COLUMN cl_product_attr.source_raw IS 'CSV生値: 元CSVの生値（そのまま）';
COMMENT ON COLUMN cl_product_attr.value_text IS '正規化名: 正規化後の名称ラベル（正しい表記）';
COMMENT ON COLUMN cl_product_attr.value_num IS '正規化数値: 数値項目用（価格・サイズなど）';
COMMENT ON COLUMN cl_product_attr.value_date IS '正規化日付: 日付項目用';
COMMENT ON COLUMN cl_product_attr.value_cd IS '正規化コード値: 正規化後のコード（g_list_item.g_item_cdなど）';
COMMENT ON COLUMN cl_product_attr.g_list_item_id IS 'GアイテムリストID';
COMMENT ON COLUMN cl_product_attr.data_type IS 'データタイプ: TEXT:テキスト、NUM:数値、DATE:日付、LIST:リスト、BOOL:真偽、REF:外部参照';
COMMENT ON COLUMN cl_product_attr.quality_flag IS '品質判定フラグ: OK:OK、REVIEW:確認、INVALID:無効';
COMMENT ON COLUMN cl_product_attr.quality_detail_json IS '品質詳細情報: 詳細な判定結果（例：{"id_label_mismatch":true,"synonym_hit":false}）';
COMMENT ON COLUMN cl_product_attr.provenance_json IS '出処情報(ルール適用履歴): どのルール/マッピングを使ったか（例：{"rule":"brand_source_map","match_type":"label"}）';
COMMENT ON COLUMN cl_product_attr.rule_version IS '適用ルールバージョン';
COMMENT ON COLUMN cl_product_attr.cre_at IS '登録日時';
COMMENT ON COLUMN cl_product_attr.upd_at IS '更新日時';

-- インデックスの作成（必要に応じて）
CREATE INDEX idx_temp_product_batch_id ON temp_product_parsed(batch_id);
CREATE INDEX idx_temp_product_step_status ON temp_product_parsed(step_status);
CREATE INDEX idx_cl_product_attr_temp_row_id ON cl_product_attr(temp_row_id);
CREATE INDEX idx_cl_product_attr_attr_cd ON cl_product_attr(attr_cd);