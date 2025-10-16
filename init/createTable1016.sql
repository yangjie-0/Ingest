-- m_company テーブル作成（GP会社マスタ）
CREATE TABLE m_company (
    group_company_id BIGINT NOT NULL,
    group_company_cd TEXT NOT NULL,
    group_company_nm TEXT,
    default_currency_cd TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_company_id)
);
COMMENT ON TABLE m_company IS 'GP会社マスタ';
COMMENT ON COLUMN m_company.group_company_id IS 'GP会社ID: 統合情報DB内サロゲートキー';
COMMENT ON COLUMN m_company.group_company_cd IS 'GP会社コード: KM,RKE,KBO等、他システム用論理値';
COMMENT ON COLUMN m_company.group_company_nm IS 'GP会社名';
COMMENT ON COLUMN m_company.default_currency_cd IS '既定通貨: JPY:日本円、USD:USドル';
COMMENT ON COLUMN m_company.is_active IS '有効フラグ';
COMMENT ON COLUMN m_company.cre_at IS '登録日時';
COMMENT ON COLUMN m_company.upd_at IS '更新日時';

-- 全局分类表
CREATE TABLE m_category_g (
    g_category_id BIGINT PRIMARY KEY,
    g_category_cd TEXT NOT NULL UNIQUE,
    g_category_id_parent BIGINT,
    g_category_nm TEXT,
    hierarchy_level BIGINT,
    g_category_sort_no SMALLINT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (g_category_id_parent) REFERENCES m_category_g(g_category_id)
);

COMMENT ON TABLE m_category_g IS '全局分类表';
COMMENT ON COLUMN m_category_g.g_category_id IS 'GカテゴリID';
COMMENT ON COLUMN m_category_g.g_category_cd IS 'Gカテゴリコード: WATCH,BAG,JEWELRY等、他システム用論理値';
COMMENT ON COLUMN m_category_g.g_category_id_parent IS '親GカテゴリID';
COMMENT ON COLUMN m_category_g.g_category_nm IS 'Gカテゴリ名';
COMMENT ON COLUMN m_category_g.hierarchy_level IS '階層レベル';
COMMENT ON COLUMN m_category_g.g_category_sort_no IS '表示順';
COMMENT ON COLUMN m_category_g.is_active IS '有効フラグ';
COMMENT ON COLUMN m_category_g.cre_at IS '登録日時';
COMMENT ON COLUMN m_category_g.upd_at IS '更新日時';

-- 全局列表组表
CREATE TABLE m_list_group_g (
    g_list_group_id BIGINT,
    g_list_group_cd TEXT,
    g_list_group_nm TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (g_list_group_id)
);

COMMENT ON TABLE m_list_group_g IS '全局列表组表';
COMMENT ON COLUMN m_list_group_g.g_list_group_id IS 'リストグループID';
COMMENT ON COLUMN m_list_group_g.g_list_group_cd IS 'リストグループコード: COLOR、MATERIAL';
COMMENT ON COLUMN m_list_group_g.g_list_group_nm IS 'リストグループ名';
COMMENT ON COLUMN m_list_group_g.is_active IS '有効フラグ';
COMMENT ON COLUMN m_list_group_g.cre_at IS '登録日時';
COMMENT ON COLUMN m_list_group_g.upd_at IS '更新日時';

-- 清理规则集主表
CREATE TABLE m_cleanse_rule_set (
    rule_set_id BIGINT PRIMARY KEY,
    rule_version TEXT NOT NULL,
    description TEXT,
    released_at TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN,
    cre_by TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE m_cleanse_rule_set IS '清理规则集主表';
COMMENT ON COLUMN m_cleanse_rule_set.rule_set_id IS 'ルールセットID';
COMMENT ON COLUMN m_cleanse_rule_set.rule_version IS 'ルールバージョン（例: ''v2025.10.01''）';
COMMENT ON COLUMN m_cleanse_rule_set.description IS 'バージョンの説明';
COMMENT ON COLUMN m_cleanse_rule_set.released_at IS 'ルール適用開始日時';
COMMENT ON COLUMN m_cleanse_rule_set.is_active IS '有効フラグ';
COMMENT ON COLUMN m_cleanse_rule_set.cre_by IS '作成者';
COMMENT ON COLUMN m_cleanse_rule_set.cre_at IS '登録日時';
COMMENT ON COLUMN m_cleanse_rule_set.upd_at IS '更新日時';

-- 全局品牌表
CREATE TABLE m_brand_g (
    g_brand_id BIGINT PRIMARY KEY,
    g_brand_cd TEXT NOT NULL,
    g_brand_nm TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE m_brand_g IS '全局品牌表';
COMMENT ON COLUMN m_brand_g.g_brand_id IS 'GブランドID: 統合情報DB内サロゲートキー';
COMMENT ON COLUMN m_brand_g.g_brand_cd IS 'Gブランドコード: ROLEX,LV等、他システム用論理値';
COMMENT ON COLUMN m_brand_g.g_brand_nm IS 'Gブランド名';
COMMENT ON COLUMN m_brand_g.is_active IS '有効フラグ';
COMMENT ON COLUMN m_brand_g.cre_at IS '登録日時';
COMMENT ON COLUMN m_brand_g.upd_at IS '更新日時';

-- 店铺表
CREATE TABLE m_store (
    store_id BIGINT,
    group_company_id BIGINT NOT NULL,
    source_store_id TEXT NOT NULL,
    source_store_nm TEXT,
    location_type_cd TEXT,
    store_nm_official TEXT,
    store_nm_search TEXT,
    store_nm_list TEXT,
    store_nm_detail TEXT,
    address_full TEXT,
    area_l_cd TEXT,
    area_m_cd TEXT,
    area_s_cd TEXT,
    is_search_target BOOLEAN,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    store_remarks TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id),
    FOREIGN KEY (group_company_id) REFERENCES m_company(group_company_id)
);

COMMENT ON TABLE m_store IS '店铺表';
COMMENT ON COLUMN m_store.store_id IS '店舗ID';
COMMENT ON COLUMN m_store.group_company_id IS 'GP会社ID';
COMMENT ON COLUMN m_store.source_store_id IS '連携元店舗ID';
COMMENT ON COLUMN m_store.source_store_nm IS '連携元店舗名';
COMMENT ON COLUMN m_store.location_type_cd IS 'ロケーション区分: PHYSICAL_STORE:実店舗、OUTLET:アウトレット、EC:EC、WAREHOUSE:倉庫、EVENT:催事、HQ:本部管理拠点';
COMMENT ON COLUMN m_store.store_nm_official IS 'ロケーション名１（正式名称）';
COMMENT ON COLUMN m_store.store_nm_search IS 'ロケーション名２（店舗検索表示用）';
COMMENT ON COLUMN m_store.store_nm_list IS 'ロケーション名４（一覧表示用）';
COMMENT ON COLUMN m_store.store_nm_detail IS 'ロケーション名４（明細表示用）';
COMMENT ON COLUMN m_store.address_full IS '所在地';
COMMENT ON COLUMN m_store.area_l_cd IS 'エリア大: item使用しない';
COMMENT ON COLUMN m_store.area_m_cd IS 'エリア中: item使用しない';
COMMENT ON COLUMN m_store.area_s_cd IS 'エリア小: item使用しない';
COMMENT ON COLUMN m_store.is_search_target IS '店舗検索表示対象';
COMMENT ON COLUMN m_store.is_active IS '有効フラグ';
COMMENT ON COLUMN m_store.store_remarks IS '備考';
COMMENT ON COLUMN m_store.cre_at IS '登録日時';
COMMENT ON COLUMN m_store.upd_at IS '更新日時';

-- 全局列表项表
CREATE TABLE m_list_item_g (
    g_list_item_id BIGINT PRIMARY KEY,
    g_list_group_id BIGINT NOT NULL,
    g_item_cd TEXT NOT NULL,
    g_item_label TEXT,
    synonyms_json JSONB NOT NULL DEFAULT '{}',
    sort_order INT DEFAULT 100,
    list_item_status TEXT NOT NULL DEFAULT 'ACTIVE',
    is_system BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (g_list_group_id) REFERENCES m_list_group_g(g_list_group_id),
    UNIQUE (g_list_group_id, g_item_cd)
);

COMMENT ON TABLE m_list_item_g IS '全局列表项表';
COMMENT ON COLUMN m_list_item_g.g_list_item_id IS 'GアイテムリストID';
COMMENT ON COLUMN m_list_item_g.g_list_group_id IS 'リストグループID';
COMMENT ON COLUMN m_list_item_g.g_item_cd IS 'G項目コード: 一意で判別可能なコード、BL、SS、PG×SS等';
COMMENT ON COLUMN m_list_item_g.g_item_label IS 'G項目表示用ラベル: 表示用';
COMMENT ON COLUMN m_list_item_g.synonyms_json IS '別名用: 別名、表記ゆれ';
COMMENT ON COLUMN m_list_item_g.sort_order IS '表示順';
COMMENT ON COLUMN m_list_item_g.list_item_status IS '状態: ACTIVE:承認 、PROVISIONAL:暫定、RETIRED:廃止';
COMMENT ON COLUMN m_list_item_g.is_system IS 'システム内部利用: SYS内部値用フラグ';
COMMENT ON COLUMN m_list_item_g.is_active IS '有効フラグ';
COMMENT ON COLUMN m_list_item_g.cre_at IS '登録日時';
COMMENT ON COLUMN m_list_item_g.upd_at IS '更新日時';


-- 属性定义表
CREATE TABLE m_attr_definition (
    attr_id BIGINT PRIMARY KEY,
    attr_cd TEXT NOT NULL UNIQUE,
    attr_nm TEXT NOT NULL,
    attr_sort_no SMALLINT,
    g_category_cd TEXT,
    data_type TEXT NOT NULL,
    g_list_group_cd TEXT,
    select_type TEXT,
    is_golden_attr BOOLEAN,
    cleanse_phase SMALLINT DEFAULT 1,
    required_context_keys TEXT[] DEFAULT '{}',
    target_table TEXT,
    target_column TEXT,
    product_unit_cd TEXT,
    credit_active_flag BOOLEAN DEFAULT FALSE,
    usage TEXT,
    table_type_cd TEXT,
    is_golden_product BOOLEAN NOT NULL,
    is_golden_attr_eav BOOLEAN NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    attr_remarks TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (g_category_cd) REFERENCES m_category_g(g_category_cd)
    -- FOREIGN KEY (g_list_group_cd) REFERENCES m_list_group_g(g_list_group_cd)
);

COMMENT ON TABLE m_attr_definition IS '属性定义表';
COMMENT ON COLUMN m_attr_definition.attr_id IS '項目ID';
COMMENT ON COLUMN m_attr_definition.attr_cd IS '項目コード';
COMMENT ON COLUMN m_attr_definition.attr_nm IS '項目名称';
COMMENT ON COLUMN m_attr_definition.attr_sort_no IS '表示順';
COMMENT ON COLUMN m_attr_definition.g_category_cd IS 'Gカテゴリコード';
COMMENT ON COLUMN m_attr_definition.data_type IS 'データタイプ: TEXT:テキスト、NUM:数値、DATE:日付、LIST:リスト、BOOL:真偽、REF:外部参照';
COMMENT ON COLUMN m_attr_definition.g_list_group_cd IS 'リストグループコード';
COMMENT ON COLUMN m_attr_definition.select_type IS 'セレクトタイプ: SINGLE:単、MULTI:複';
COMMENT ON COLUMN m_attr_definition.is_golden_attr IS '正規化厳密対象';
COMMENT ON COLUMN m_attr_definition.cleanse_phase IS '優先度';
COMMENT ON COLUMN m_attr_definition.required_context_keys IS '処理設定';
COMMENT ON COLUMN m_attr_definition.target_table IS '保存対象テーブル';
COMMENT ON COLUMN m_attr_definition.target_column IS '保存対象カラム';
COMMENT ON COLUMN m_attr_definition.product_unit_cd IS '単位コード: cm:センチメートル、mm:ミリメートル、ct:カラット、g:グラム';
COMMENT ON COLUMN m_attr_definition.credit_active_flag IS '単位適用フラグ';
COMMENT ON COLUMN m_attr_definition.usage IS '用途: PRODUCT:商品のみ、CATALOG:カタログのみ、NULL:両方';
COMMENT ON COLUMN m_attr_definition.table_type_cd IS 'テーブル種別コード: MST:マスタ、EAV:EAV項目';
COMMENT ON COLUMN m_attr_definition.is_golden_product IS 'G商品レコード昇格フラグ: m_product昇格対象';
COMMENT ON COLUMN m_attr_definition.is_golden_attr IS 'G商品レコードEAV昇格フラグ: m_product_eav保存対象';
COMMENT ON COLUMN m_attr_definition.is_active IS '有効フラグ';
COMMENT ON COLUMN m_attr_definition.attr_remarks IS '備考';
COMMENT ON COLUMN m_attr_definition.cre_at IS '登録日時';
COMMENT ON COLUMN m_attr_definition.upd_at IS '更新日時';

-- 属性清理策略表
CREATE TABLE m_attr_cleanse_policy (
    policy_id BIGINT PRIMARY KEY,
    rule_set_id BIGINT NOT NULL,
    attr_cd TEXT NOT NULL,
    data_type TEXT NOT NULL,
    ref_map_id BIGINT,
    g_list_group_cd TEXT,
    gp_scope TEXT,
    category_scope TEXT,
    brand_scope TEXT,
    step_no SMALLINT NOT NULL,
    matcher_kind TEXT NOT NULL,
    derive_from_attr_cds TEXT[],
    split_mode TEXT,
    stop_on_hit BOOLEAN DEFAULT TRUE,
    threshold NUMERIC(5,2),
    is_active BOOLEAN DEFAULT TRUE,
    import_remarks TEXT,
    cre_by TEXT,
    upd_by TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (rule_set_id) REFERENCES m_cleanse_rule_set(rule_set_id)
    -- FOREIGN KEY (g_list_group_cd) REFERENCES m_list_group_g(g_list_group_cd)
);

COMMENT ON TABLE m_attr_cleanse_policy IS '属性清理策略表';
COMMENT ON COLUMN m_attr_cleanse_policy.policy_id IS 'ポリシーID: 複数ルールを束ねるポリシーID';
COMMENT ON COLUMN m_attr_cleanse_policy.rule_set_id IS 'ルールセットID';
COMMENT ON COLUMN m_attr_cleanse_policy.attr_cd IS '項目コード';
COMMENT ON COLUMN m_attr_cleanse_policy.data_type IS 'データタイプ: TEXT:テキスト、NUM:数値、DATE:日付、LIST:リスト、BOOL:真偽、REF:外部参照 取込設定の上書き';
COMMENT ON COLUMN m_attr_cleanse_policy.ref_map_id IS '参照マップID';
COMMENT ON COLUMN m_attr_cleanse_policy.g_list_group_cd IS 'リストグループコード: COLOR、MATERIAL';
COMMENT ON COLUMN m_attr_cleanse_policy.gp_scope IS '限定対象GP会社: 同一項目だがGP会社で異なるルールがある場合';
COMMENT ON COLUMN m_attr_cleanse_policy.category_scope IS '限定対象カテゴリ: 同一項目だがカテゴリで異なるルールがある場合';
COMMENT ON COLUMN m_attr_cleanse_policy.brand_scope IS '限定対象ブランド: 同一項目だがブランドで異なるルールがある場合';
COMMENT ON COLUMN m_attr_cleanse_policy.step_no IS '優先順位: 同一項目コードのルール優先度';
COMMENT ON COLUMN m_attr_cleanse_policy.matcher_kind IS '照合方式: ID_EXACT:ID完全一致、LABEL_EXACT:ラベル完全一致、SYNONYM:同義語辞書一致、NORMALIZE_EQ:正規化ルール一致、LIST_LOOKUP:辞書走査・部分一致、FUZZY_MATCH:曖昧照合・類似度マッチ、DERIVE_COALESCE:派生作成';
COMMENT ON COLUMN m_attr_cleanse_policy.derive_from_attr_cds IS '派生優先順リスト';
COMMENT ON COLUMN m_attr_cleanse_policy.split_mode IS '値分割記号: NULL=単値';
COMMENT ON COLUMN m_attr_cleanse_policy.stop_on_hit IS '打ち切り: マッチ成功したら終了';
COMMENT ON COLUMN m_attr_cleanse_policy.threshold IS '類似度';
COMMENT ON COLUMN m_attr_cleanse_policy.is_active IS '有効フラグ';
COMMENT ON COLUMN m_attr_cleanse_policy.import_remarks IS '備考';
COMMENT ON COLUMN m_attr_cleanse_policy.cre_by IS '作成者: 監査用';
COMMENT ON COLUMN m_attr_cleanse_policy.upd_by IS '更新者: 監査用';
COMMENT ON COLUMN m_attr_cleanse_policy.cre_at IS '登録日時';
COMMENT ON COLUMN m_attr_cleanse_policy.upd_at IS '更新日時';

-- 参照表映射表
CREATE TABLE m_ref_table_map (
    ref_map_id BIGINT PRIMARY KEY,
    attr_cd TEXT NOT NULL,
    data_source TEXT NOT NULL,
    hop1_table TEXT NOT NULL,
    hop1_match_by TEXT NOT NULL,
    hop1_id_col TEXT,
    hop1_label_col TEXT,
    hop1_return_cols TEXT[] DEFAULT '{}',
    hop2_table TEXT,
    hop2_join_on_json JSONB DEFAULT '{}',
    hop2_return_cd_col TEXT,
    hop2_return_label_col TEXT,
    ref_table_remarks TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (attr_cd) REFERENCES m_attr_definition(attr_cd)
);

COMMENT ON TABLE m_ref_table_map IS '参照表映射表';
COMMENT ON COLUMN m_ref_table_map.ref_map_id IS '参照マップID';
COMMENT ON COLUMN m_ref_table_map.attr_cd IS '項目コード';
COMMENT ON COLUMN m_ref_table_map.data_source IS 'データソース区分: MASTER:通常マスタ、DICT:辞書、EXT:外部API';
COMMENT ON COLUMN m_ref_table_map.hop1_table IS '参照１テーブル: m_company, m_brand, m_store, m_category';
COMMENT ON COLUMN m_ref_table_map.hop1_match_by IS '参照１テーブルマッチ方式: ID:ID一致、LABEL:名称一致、AUTO:自動（ID優先、無ければ名称）';
COMMENT ON COLUMN m_ref_table_map.hop1_id_col IS '参照１テーブル照合ID';
COMMENT ON COLUMN m_ref_table_map.hop1_label_col IS '参照１テーブル照合名';
COMMENT ON COLUMN m_ref_table_map.hop1_return_cols IS '参照１テーブル返却値';
COMMENT ON COLUMN m_ref_table_map.hop2_table IS '参照２テーブル';
COMMENT ON COLUMN m_ref_table_map.hop2_join_on_json IS '参照１参照２JOIN';
COMMENT ON COLUMN m_ref_table_map.hop2_return_cd_col IS '参照２テーブル最終返却コード列（cl_product_attr.value_code の元）';
COMMENT ON COLUMN m_ref_table_map.hop2_return_label_col IS '参照２テーブル最終返却ラベル列（cl_product_attr.value_text の元）';
COMMENT ON COLUMN m_ref_table_map.ref_table_remarks IS '備考';
COMMENT ON COLUMN m_ref_table_map.is_active IS '有効フラグ';
COMMENT ON COLUMN m_ref_table_map.cre_at IS '登録日時';
COMMENT ON COLUMN m_ref_table_map.upd_at IS '更新日時';

-- 品牌源映射表
CREATE TABLE brand_source_map (
    map_id BIGINT PRIMARY KEY,
    group_company_cd TEXT NOT NULL,
    source_brand_id TEXT,
    source_brand_nm TEXT,
    source_brand_nm_n TEXT,
    g_brand_id BIGINT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source_map_remarks TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (g_brand_id) REFERENCES m_brand_g(g_brand_id)
);

COMMENT ON TABLE brand_source_map IS '品牌源映射表';
COMMENT ON COLUMN brand_source_map.map_id IS 'マップID: サロゲート';
COMMENT ON COLUMN brand_source_map.group_company_cd IS 'GP会社コード: KM,RKE,KBO等、他システム用論理値';
COMMENT ON COLUMN brand_source_map.source_brand_id IS '連携元ブランドID';
COMMENT ON COLUMN brand_source_map.source_brand_nm IS '連携元ブランド名';
COMMENT ON COLUMN brand_source_map.source_brand_nm_n IS '連携元ブランド名(正規化): 正規化済み名（trim/全半角/大文字化 等）';
COMMENT ON COLUMN brand_source_map.g_brand_id IS 'GブランドID';
COMMENT ON COLUMN brand_source_map.is_active IS '有効フラグ';
COMMENT ON COLUMN brand_source_map.source_map_remarks IS '備考';
COMMENT ON COLUMN brand_source_map.cre_at IS '登録日時';
COMMENT ON COLUMN brand_source_map.upd_at IS '更新日時';


-- 属性源映射表
CREATE TABLE attr_source_map (
    map_id BIGINT PRIMARY KEY,
    group_company_cd TEXT NOT NULL,
    g_list_group_id BIGINT,
    g_brand_cd TEXT,
    g_category_cd TEXT,
    usage TEXT,
    source_attr_id TEXT,
    source_attr_nm TEXT,
    match_mode TEXT DEFAULT 'AUTO',
    g_list_item_id BIGINT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    upd_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (g_list_group_id) REFERENCES m_list_group_g(g_list_group_id),
    FOREIGN KEY (g_list_item_id) REFERENCES m_list_item_g(g_list_item_id)
);

COMMENT ON TABLE attr_source_map IS '属性源映射表';
COMMENT ON COLUMN attr_source_map.map_id IS 'マップID: サロゲート';
COMMENT ON COLUMN attr_source_map.group_company_cd IS 'GP会社コード: KM,RKE,KBO等、他システム用論理値';
COMMENT ON COLUMN attr_source_map.g_list_group_id IS 'リストグループID';
COMMENT ON COLUMN attr_source_map.g_brand_cd IS 'Gブランドコード: ブランド＋カテゴリ辞書を変える用';
COMMENT ON COLUMN attr_source_map.g_category_cd IS 'Gカテゴリコード: ブランド＋カテゴリ辞書を変える用';
COMMENT ON COLUMN attr_source_map.usage IS 'ユースジ: PRODUCT、CATALOG';
COMMENT ON COLUMN attr_source_map.source_attr_id IS '連携元ID: KMにIDなし項目あり';
COMMENT ON COLUMN attr_source_map.source_attr_nm IS '連携元名称';
COMMENT ON COLUMN attr_source_map.match_mode IS 'マッチモード: ID:IDのみで一致、NAME:名前だけで一致、BOTH:IDと名前で一致 AUTO:IDがあればID優先';
COMMENT ON COLUMN attr_source_map.g_list_item_id IS 'GアイテムリストID';
COMMENT ON COLUMN attr_source_map.is_active IS '有効フラグ';
COMMENT ON COLUMN attr_source_map.cre_at IS '登録日時';
COMMENT ON COLUMN attr_source_map.upd_at IS '更新日時';





