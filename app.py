# --- HỆ THỐNG BHXH CHUYÊN NGHIỆP (PHIÊN BẢN LITE - GIAO DIỆN MỚI) ---
import streamlit as st
import pandas as pd
import sqlite3
import unidecode
import time
import os
import zipfile
import glob
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Việt Nam",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🎨 CẤU HÌNH GIAO DIỆN & CSS (UI/UX)
# ==============================================================================
# Màu xanh chủ đạo của BHXH: #1f77b4 (hoặc #005b96)
BHXH_BLUE = "#005b96"
BHXH_LIGHT_BLUE = "#e6f2ff"

st.markdown(f"""
    <style>
    /* Tổng thể */
    .main {{
        background-color: #f8f9fa;
    }}
    h1, h2, h3 {{
        color: {BHXH_BLUE} !important;
        font-family: 'Arial', sans-serif;
    }}
    
    /* Header Container */
    .header-text h1 {{
        margin: 0;
        font-size: 1.8rem;
        text-transform: uppercase;
        color: {BHXH_BLUE};
    }}
    .header-text p {{
        margin: 0;
        font-style: italic;
        color: #555;
        font-weight: 500;
    }}

    /* Card/Container style */
    .stExpander, .stDataFrame {{
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }}
    
    /* Button Style */
    .stButton>button {{
        background-color: {BHXH_BLUE};
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        transition: all 0.3s;
    }}
    .stButton>button:hover {{
        background-color: #004470;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background-color: {BHXH_LIGHT_BLUE};
        border-right: 1px solid #ddd;
    }}
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
# 🔑 CẤU HÌNH HỆ THỐNG
# ==============================================================================
ZALO_PHONE_NUMBER = "0986053006" 

# CÁC HẰNG SỐ TÍNH BHXH TỰ NGUYỆN (CẬP NHẬT 2025)
CHUAN_NGHEO = 1500000 
LUONG_CO_SO = 2340000 
MAX_MUC_DONG = 20 * LUONG_CO_SO 
TY_LE_DONG = 0.22 

# Mức hỗ trợ
HO_TRO_NGHEO = 0.50     
HO_TRO_CAN_NGHEO = 0.40 
HO_TRO_DAN_TOC = 0.30   
HO_TRO_KHAC = 0.20      

# Tên file dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 

# --- HEADER FUNCTION (ĐÃ FIX LOGO) ---
def render_header():
    # Sử dụng st.columns để căn chỉnh logo và text đẹp hơn
    c1, c2 = st.columns([1, 8])
    with c1:
        # Logo BHXH Việt Nam
        st.image("https://upload.wikimedia.org/wikipedia/vi/thumb/a/a2/Logo_BHXH_VN.png/300px-Logo_BHXH_VN.png", width=90)
    with c2:
        st.markdown(f"""
            <div class="header-text" style="padding-top: 10px;">
                <h1>BẢO HIỂM XÃ HỘI VIỆT NAM</h1>
                <p>Tất cả vì an sinh xã hội, vì người tham gia BHXH, BHYT</p>
            </div>
            <hr style="margin-top: 5px; border-top: 2px solid {BHXH_BLUE};">
        """, unsafe_allow_html=True)

# --- WIDGET ZALO ---
def render_zalo_widget():
    st.markdown(f"""<style>.z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}@keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}</style><a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%"></a>""", unsafe_allow_html=True)

# --- XỬ LÝ DỮ LIỆU (GIỮ NGUYÊN) ---
def clean_text(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_and_prepare_data():
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db(); res = conn.execute("SELECT count(*) FROM bhxh").fetchone(); conn.close()
            if res and res[0] > 0: return True, "Sẵn sàng"
        except: os.remove(DB_FILE)
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty(); msg.info(f"📦 Đang nối {len(parts)} phần dữ liệu...")
        try:
            with open("bhxh_full.zip", 'wb') as o:
                for p in parts: 
                    with open(p, 'rb') as i: o.write(i.read())
            msg.info("📦 Đang giải nén..."); 
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z: z.extractall()
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            msg.empty(); return True, "Restored"
        except Exception as e: return False, str(e)
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "⚠️ Thiếu dữ liệu"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel. Nên dùng tool chia nhỏ file.")
    conn = init_data_db(); msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang xử lý..."); df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); bar.progress(40)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        for col in df.columns: 
            if col != 'master_search_idx': df[f'idx_{col}'] = df[col].apply(clean_text)
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        bar.progress(100); msg.empty(); bar.empty(); conn.close(); return True, "Done"
    except Exception as e: conn.close(); return False, str(e)

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor(); c.execute("PRAGMA table_info(bhxh)")
        all = [r[1] for r in c.fetchall()]
        return [c for c in all if not c.startswith('idx_') and c != 'master_search_idx' and 'kcb' not in c.lower() and c != 'index']
    except: return []
    finally: conn.close()

# --- TÌM KIẾM (GIỮ NGUYÊN LOGIC) ---
def search_data(mode, q):
    conn = init_data_db(); cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        if mode == 'simple': 
            k = clean_text(q); 
            if not k: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{k}%',))
        else:
            conds, vals = [], []
            for c, v in q.items():
                if v.strip():
                    conds.append(f'idx_{unidecode.unidecode(c).strip().replace(" ", "_").lower()} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- TIỆN ÍCH FORMAT TIỀN ---
def format_vnd(value):
    return f"{int(value):,} VNĐ".replace(",", ".")

# --- 1. TÍNH BHXH TỰ NGUYỆN ---
def render_calculator():
    st.subheader("🧮 Tính Mức Đóng BHXH Tự Nguyện")
    st.caption("Công cụ ước tính số tiền đóng BHXH tự nguyện (Cập nhật 2025).")

    # Nhập liệu
    col_inp, col_info = st.columns([2, 1])
    with col_inp:
        income = st.slider(
            "Mức thu nhập lựa chọn:", 
            min_value=CHUAN_NGHEO, 
            max_value=MAX_MUC_DONG, 
            value=CHUAN_NGHEO,
            step=50000,
            format="%d"
        )
        st.info(f"Thu nhập chọn đóng: **{format_vnd(income)}**")
        
        exact_income = st.number_input("Hoặc nhập số chính xác:", min_value=CHUAN_NGHEO, max_value=MAX_MUC_DONG, value=income, step=1000)
        if exact_income != income: income = exact_income

    with col_info:
        st.info(f"""
        **Thông số cơ sở:**
        \n- Chuẩn nghèo: {format_vnd(CHUAN_NGHEO)}
        \n- Tỷ lệ đóng: 22%
        \n- Hỗ trợ tối đa: 10 năm
        """)

    # Chọn đối tượng
    doi_tuong = st.radio(
        "Đối tượng ưu tiên:",
        ["Khác (Hỗ trợ 20%)", "Hộ nghèo (Hỗ trợ 50%)", "Hộ cận nghèo (Hỗ trợ 40%)", "Dân tộc thiểu số (Hỗ trợ 30%)"],
        horizontal=True
    )

    # Tính toán
    muc_dong_chuan = income * TY_LE_DONG
    if "Hộ nghèo" in doi_tuong: tile_hotro, hs = "50%", HO_TRO_NGHEO
    elif "Hộ cận nghèo" in doi_tuong: tile_hotro, hs = "40%", HO_TRO_CAN_NGHEO
    elif "Dân tộc" in doi_tuong: tile_hotro, hs = "30%", HO_TRO_DAN_TOC
    else: tile_hotro, hs = "20%", HO_TRO_KHAC

    muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * hs
    so_tien_thuc_dong = muc_dong_chuan - muc_ho_tro

    # Kết quả
    st.markdown(f"#### 📊 Bảng Chi Tiết (Hỗ trợ: {tile_hotro})")
    data = {"Phương thức": [], "Số tháng": [1, 3, 6, 12], "Tổng đóng (Gốc)": [], "Được hỗ trợ": [], "SỐ TIỀN PHẢI ĐÓNG": []}
    
    modes = ["Hằng tháng", "3 tháng", "6 tháng", "12 tháng"]
    for i, m in enumerate(data["Số tháng"]):
        data["Phương thức"].append(modes[i])
        data["Tổng đóng (Gốc)"].append(format_vnd(muc_dong_chuan * m))
        data["Được hỗ trợ"].append(format_vnd(muc_ho_tro * m))
        data["SỐ TIỀN PHẢI ĐÓNG"].append(format_vnd(so_tien_thuc_dong * m))

    st.dataframe(pd.DataFrame(data).style.highlight_max(axis=0, subset=["SỐ TIỀN PHẢI ĐÓNG"], color='#dbeeff'), use_container_width=True, hide_index=True)

# --- 2. TÍNH BHYT HỘ GIA ĐÌNH (UPDATE: 3, 6, 12 THÁNG) ---
def render_bhyt_calculator():
    st.subheader("🏥 Tính Tiền BHYT Hộ Gia Đình")
    st.caption(f"Áp dụng mức lương cơ sở: **{format_vnd(LUONG_CO_SO)}** | Mức đóng: **4.5%**")

    c1, c2 = st.columns([1, 2])
    
    total_household_3m = 0
    total_household_6m = 0
    total_household_12m = 0
    
    with c1:
        num_people = st.number_input("Số người trong hộ gia đình tham gia:", min_value=1, max_value=20, value=1, step=1)
        
        # Tính toán
        base_rate_monthly = LUONG_CO_SO * 0.045 # 100% mức đóng 1 tháng
        details = []

        for i in range(1, num_people + 1):
            if i == 1:
                rate = 1.0
                note = "100% mức đóng"
            elif i == 2:
                rate = 0.7
                note = "70% người thứ 1"
            elif i == 3:
                rate = 0.6
                note = "60% người thứ 1"
            elif i == 4:
                rate = 0.5
                note = "50% người thứ 1"
            else:
                rate = 0.4
                note = "40% người thứ 1"
            
            # Tiền đóng 1 tháng
            cost_1m = base_rate_monthly * rate
            
            # Tính các mốc
            cost_3m = cost_1m * 3
            cost_6m = cost_1m * 6
            cost_12m = cost_1m * 12
            
            # Cộng dồn tổng hộ
            total_household_3m += cost_3m
            total_household_6m += cost_6m
            total_household_12m += cost_12m
            
            details.append({
                "Thành viên": f"Người thứ {i}",
                "Mức giảm": note,
                "03 Tháng": format_vnd(cost_3m),
                "06 Tháng": format_vnd(cost_6m),
                "12 Tháng": format_vnd(cost_12m)
            })

    with c2:
        st.markdown(f"""
        <div style="background-color: #e6f2ff; padding: 15px; border-radius: 10px; border: 1px solid #005b96; text-align: center;">
            <h4 style="color: #005b96; margin-bottom: 10px;">TỔNG SỐ TIỀN CẢ HỘ PHẢI ĐÓNG</h4>
            <div style="display: flex; justify-content: space-around; text-align: center;">
                <div>
                    <span style="font-size: 0.9em; color: #555;">03 Tháng</span><br>
                    <strong style="font-size: 1.2em; color: #d9534f;">{format_vnd(total_household_3m)}</strong>
                </div>
                <div>
                    <span style="font-size: 0.9em; color: #555;">06 Tháng</span><br>
                    <strong style="font-size: 1.2em; color: #d9534f;">{format_vnd(total_household_6m)}</strong>
                </div>
                <div>
                    <span style="font-size: 0.9em; color: #555;">12 Tháng</span><br>
                    <strong style="font-size: 1.4em; color: #d9534f;">{format_vnd(total_household_12m)}</strong>
                </div>
            </div>
            <p style="margin-top: 10px; font-size: 0.85em; color: #666;">(Áp dụng cho {num_people} thành viên)</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("### 📋 Chi tiết mức đóng từng thành viên")
    df_bhyt = pd.DataFrame(details)
    st.dataframe(df_bhyt, use_container_width=True, hide_index=True)
    
    st.info("💡 **Lưu ý:** Người thứ 5 trở đi đóng bằng 40% mức đóng của người thứ nhất.")

# --- 3. TÍNH TUỔI NGHỈ HƯU (GIỮ NGUYÊN) ---
def render_retirement_calculator():
    st.subheader("👴👵 Tính Tuổi Nghỉ Hưu (NĐ 135/2020)")
    c1, c2 = st.columns(2)
    with c1:
        dob = st.date_input("Ngày sinh:", min_value=date(1950, 1, 1), max_value=date(2010, 12, 31), value=date(1970, 1, 1))
    with c2:
        gender = st.radio("Giới tính:", ["Nam", "Nữ"], horizontal=True)

    if st.button("Xác định thời điểm nghỉ hưu", type="primary"):
        target_years, target_months = 0, 0
        
        # Logic NĐ 135
        if gender == "Nam":
            if dob < date(1961, 1, 1): target_years = 60
            elif dob >= date(1966, 10, 1): target_years = 62
            else:
                target_years, target_months = 60, (dob.year - 1960) * 3
                if dob.year == 1966 and dob.month >= 10: target_years, target_months = 62, 0
        else:
            if dob < date(1966, 1, 1): target_years = 55
            elif dob >= date(1980, 1, 1): target_years = 60
            else:
                target_years, target_months = 55, (dob.year - 1965) * 4

        add_years = target_months // 12
        final_age_years = target_years + add_years
        final_age_months = target_months % 12
        
        retirement_date = dob + relativedelta(years=final_age_years, months=final_age_months)
        
        st.success(f"✅ **Tuổi nghỉ hưu:** {final_age_years} tuổi {final_age_months} tháng")
        st.info(f"📅 **Thời điểm nghỉ hưu:** Tháng {retirement_date.month}/{retirement_date.year}")

# --- GIAO DIỆN TÌM KIẾM (GIỮ NGUYÊN) ---
def render_search(cols):
    st.subheader("🔍 Tra Cứu Thông Tin")
    t1, t2 = st.tabs(["Tra cứu nhanh", "Tra cứu chi tiết"])
    with t1:
        q = st.text_input("Nhập từ khóa (Tên, Năm sinh...):", placeholder="vd: nguyen van a 1990")
        if q:
            df = search_data('simple', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
            else: st.warning("Không tìm thấy kết quả nào.")
    with t2:
        defs = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        sel = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defs)] or cols[:4] 
        with st.expander("Bộ lọc nâng cao", expanded=True): s = st.multiselect("Chọn trường:", cols, default=sel)
        inp = {}
        if s:
            c = st.columns(4)
            for i, n in enumerate(s): inp[n] = c[i % 4].text_input(n)
        if st.button("Tìm kiếm ngay"):
            v = {k: val for k, val in inp.items() if val.strip()}
            if v:
                df = search_data('manual', v)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else: st.warning("Không có kết quả.")

# --- MAIN ---
def main():
    render_header() # Render Logo Header
    if 'page' not in st.session_state: st.session_state['page'] = 'search'
    
    render_zalo_widget()
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    
    # Sidebar
    with st.sidebar:
        st.title("MENU CHỨC NĂNG")
        st.markdown("---")
        if st.button("🔍 Tra cứu CSDL", use_container_width=True): st.session_state['page'] = 'search'
        if st.button("🧮 Tính BHXH Tự Nguyện", use_container_width=True): st.session_state['page'] = 'calc'
        if st.button("🏥 Tính BHYT Hộ Gia Đình", use_container_width=True): st.session_state['page'] = 'bhyt' # Nút mới
        if st.button("👵 Tính Tuổi Nghỉ Hưu", use_container_width=True): st.session_state['page'] = 'retirement'
        
        st.markdown("---")
        st.info("Hệ thống hỗ trợ tra cứu và tính toán BHXH, BHYT mới nhất.")

    # Router
    p = st.session_state['page']
    if p == 'search': 
        cols = get_display_columns()
        if cols: render_search(cols)
    elif p == 'calc': render_calculator()
    elif p == 'bhyt': render_bhyt_calculator() # Trang mới
    elif p == 'retirement': render_retirement_calculator()

if __name__ == '__main__':
    main()
