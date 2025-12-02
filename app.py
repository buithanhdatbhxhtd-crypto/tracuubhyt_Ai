# --- HỆ THỐNG BHXH CHUYÊN NGHIỆP (PHIÊN BẢN LITE - KHÔNG AI) ---
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
    page_title="Hệ thống BHXH (Tra cứu & Tính toán)",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 CẤU HÌNH HỆ THỐNG
# ==============================================================================
ZALO_PHONE_NUMBER = "0986053006" 

# CÁC HẰNG SỐ TÍNH BHXH TỰ NGUYỆN (CẬP NHẬT 2025)
CHUAN_NGHEO = 1500000 # Mức chuẩn nghèo khu vực nông thôn
LUONG_CO_SO = 2340000 # Mức lương cơ sở
MAX_MUC_DONG = 20 * LUONG_CO_SO # Mức đóng tối đa
TY_LE_DONG = 0.22 # Tỷ lệ đóng 22%

# Mức hỗ trợ mới nhất
HO_TRO_NGHEO = 0.50     # 50%
HO_TRO_CAN_NGHEO = 0.40 # 40%
HO_TRO_DAN_TOC = 0.30   # 30%
HO_TRO_KHAC = 0.20      # 20%

# Tên file dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 

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
        if mode == 'simple': # Đổi tên từ 'ai' sang 'simple' cho chính xác
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

# --- TÍNH BHXH TỰ NGUYỆN (GIỮ NGUYÊN) ---
def format_vnd(value):
    return f"{int(value):,} VNĐ".replace(",", ".")

def render_calculator():
    st.subheader("🧮 Tính Mức Đóng BHXH Tự Nguyện")
    st.caption("Công cụ ước tính số tiền đóng BHXH tự nguyện theo quy định mới nhất (2025).")

    # 1. Nhập mức thu nhập
    st.markdown("#### 1. Chọn mức thu nhập làm căn cứ đóng")
    col_inp, col_info = st.columns([2, 1])
    
    with col_inp:
        # Thanh trượt chọn mức thu nhập (Bước nhảy 50k)
        income = st.slider(
            "Mức thu nhập (kéo thanh trượt):", 
            min_value=CHUAN_NGHEO, 
            max_value=MAX_MUC_DONG, 
            value=CHUAN_NGHEO,
            step=50000,
            format="%d"
        )
        st.info(f"Mức thu nhập bạn chọn: **{format_vnd(income)}**")
        
        # Nhập số chính xác nếu cần
        exact_income = st.number_input("Hoặc nhập số chính xác:", min_value=CHUAN_NGHEO, max_value=MAX_MUC_DONG, value=income, step=1000)
        if exact_income != income:
            income = exact_income

    with col_info:
        st.markdown(
            f"""
            <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; font-size: 0.9em;">
            <b>Thông tin tham chiếu:</b><br>
            - Chuẩn nghèo: {format_vnd(CHUAN_NGHEO)}<br>
            - Tối đa (20 lần LCS): {format_vnd(MAX_MUC_DONG)}<br>
            - Tỷ lệ đóng: 22%
            </div>
            """, unsafe_allow_html=True
        )

    # 2. Chọn đối tượng
    st.markdown("#### 2. Chọn đối tượng ưu tiên (để tính mức hỗ trợ)")
    doi_tuong = st.radio(
        "Bạn thuộc đối tượng nào?",
        ["Khác (Hỗ trợ 20%)", "Hộ nghèo (Hỗ trợ 50%)", "Hộ cận nghèo (Hỗ trợ 40%)", "Dân tộc thiểu số (Hỗ trợ 30%)"],
        horizontal=True
    )

    # Tính toán
    muc_dong_chuan = income * TY_LE_DONG
    
    if "Hộ nghèo" in doi_tuong:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_NGHEO
        tile_hotro = "50%"
    elif "Hộ cận nghèo" in doi_tuong:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_CAN_NGHEO
        tile_hotro = "40%"
    elif "Dân tộc" in doi_tuong:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_DAN_TOC
        tile_hotro = "30%"
    else:
        muc_ho_tro = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_KHAC
        tile_hotro = "20%"

    so_tien_thuc_dong = muc_dong_chuan - muc_ho_tro

    # 3. Hiển thị kết quả
    st.markdown("---")
    st.markdown(f"#### 📊 Bảng Chi Tiết Số Tiền Phải Đóng (Hỗ trợ: {tile_hotro})")
    
    data = {
        "Phương thức": ["Hằng tháng", "3 tháng", "6 tháng", "12 tháng"],
        "Số tháng": [1, 3, 6, 12],
        "Tổng mức đóng (chưa giảm)": [],
        "Nhà nước hỗ trợ": [],
        "BẠN PHẢI ĐÓNG": []
    }

    for months in data["Số tháng"]:
        total_raw = muc_dong_chuan * months
        total_support = muc_ho_tro * months
        total_final = so_tien_thuc_dong * months
        
        data["Tổng mức đóng (chưa giảm)"].append(format_vnd(total_raw))
        data["Nhà nước hỗ trợ"].append(format_vnd(total_support))
        data["BẠN PHẢI ĐÓNG"].append(format_vnd(total_final))

    df_result = pd.DataFrame(data)
    
    st.dataframe(
        df_result.style.highlight_max(axis=0, subset=["BẠN PHẢI ĐÓNG"], color='#e6ffe6'),
        use_container_width=True,
        hide_index=True
    )
    
    st.success(f"💡 **Kết luận:** Với mức thu nhập **{format_vnd(income)}**, đối tượng **{doi_tuong}**, bạn chỉ cần đóng **{format_vnd(so_tien_thuc_dong)}/tháng**.")

# --- TÍNH TUỔI NGHỈ HƯU (NEW - NGHỊ ĐỊNH 135) ---
def render_retirement_calculator():
    st.subheader("👴👵 Tính Tuổi Nghỉ Hưu (Nghị định 135/2020/NĐ-CP)")
    st.caption("Công cụ xác định thời điểm nghỉ hưu chính xác theo lộ trình tăng tuổi nghỉ hưu.")

    # Input
    c1, c2 = st.columns(2)
    with c1:
        dob = st.date_input("Ngày tháng năm sinh:", min_value=date(1950, 1, 1), max_value=date(2010, 12, 31), value=date(1970, 1, 1))
    with c2:
        gender = st.radio("Giới tính:", ["Nam", "Nữ"], horizontal=True)

    if st.button("Tính toán ngày nghỉ hưu", type="primary"):
        # LOGIC TÍNH TOÁN THEO NGHỊ ĐỊNH 135
        # NAM:
        # - Sinh trước 1/1/1961: 60 tuổi
        # - Sinh từ 1/10/1966 trở đi: 62 tuổi
        # - Lộ trình: Mỗi năm tăng 3 tháng
        # NỮ:
        # - Sinh trước 1/1/1966: 55 tuổi
        # - Sinh từ 1/1/1980 (đã điều chỉnh để khớp lộ trình): 60 tuổi 
        #   (Chính xác là sinh từ tháng 9/1979 theo bảng, nhưng tính tròn lộ trình theo năm)
        # - Lộ trình: Mỗi năm tăng 4 tháng

        target_years = 0
        target_months = 0
        
        # 1. Xác định tuổi nghỉ hưu quy định
        if gender == "Nam":
            # Mốc cố định cũ
            if dob < date(1961, 1, 1):
                target_years = 60
                target_months = 0
            # Mốc cố định mới (max)
            elif dob >= date(1966, 10, 1):
                target_years = 62
                target_months = 0
            else:
                # Giai đoạn chuyển tiếp (Sinh 1961 - 9/1966)
                # Công thức: 60 tuổi + (Năm sinh - 1960) * 3 tháng ???
                # Cách chính xác nhất là map theo năm sinh như Phụ lục I
                # 1961 -> 60t 3th
                # 1962 -> 60t 6th
                # ...
                year_diff = dob.year - 1960
                months_add = year_diff * 3
                
                # Xử lý riêng cho năm 1966 (chỉ đến tháng 9)
                if dob.year == 1966 and dob.month >= 10:
                    target_years = 62
                    target_months = 0
                else:
                    target_years = 60
                    target_months = months_add
                    
        else: # Nữ
            # Mốc cố định cũ
            if dob < date(1966, 1, 1):
                target_years = 55
                target_months = 0
            # Mốc cố định mới (max) - Theo phụ lục là từ 1980 (hoặc cuối 1979)
            elif dob >= date(1980, 1, 1): # Căn cứ thực tế lộ trình đến 2035
                target_years = 60
                target_months = 0
            else:
                # Giai đoạn chuyển tiếp (Sinh 1966 - 1979)
                # 1966 -> 55t 4th
                # 1967 -> 55t 8th
                year_diff = dob.year - 1965
                months_add = year_diff * 4
                
                target_years = 55
                target_months = months_add

        # 2. Quy đổi target_months dư thành năm (ví dụ 15 tháng -> 1 năm 3 tháng)
        add_years = target_months // 12
        rem_months = target_months % 12
        
        final_age_years = target_years + add_years
        final_age_months = rem_months

        # 3. Tính ngày nghỉ hưu
        # Logic: Cộng số năm và số tháng vào ngày sinh
        retirement_date = dob + relativedelta(years=final_age_years, months=final_age_months)
        
        # Vì thời điểm nghỉ hưu là "kết thúc tháng đủ tuổi", thời điểm hưởng là "đầu tháng liền kề"
        # Ta hiển thị tháng nghỉ hưu
        
        st.markdown("---")
        st.success("✅ **KẾT QUẢ TÍNH TOÁN**")
        
        res_col1, res_col2 = st.columns(2)
        
        with res_col1:
            st.metric(
                label="Tuổi nghỉ hưu theo quy định", 
                value=f"{final_age_years} tuổi {final_age_months} tháng" if final_age_months > 0 else f"{final_age_years} tuổi"
            )
        
        with res_col2:
            st.metric(
                label="Thời điểm được nghỉ hưu",
                value=f"Tháng {retirement_date.month}/{retirement_date.year}"
            )
            
        st.info(f"📅 Cụ thể: Bạn sinh ngày {dob.day}/{dob.month}/{dob.year}, bạn sẽ đủ tuổi nghỉ hưu vào khoảng **tháng {retirement_date.month} năm {retirement_date.year}**.")
        st.caption("Lưu ý: Kết quả này áp dụng cho điều kiện lao động bình thường (không tính trường hợp suy giảm lao động, làm nghề nặng nhọc độc hại, v.v...).")

# --- GIAO DIỆN TÌM KIẾM (GIỮ NGUYÊN) ---
def render_search(cols):
    st.subheader("🔍 Tra Cứu Thông Tin")
    t1, t2 = st.tabs(["Tra cứu nhanh", "Tra cứu chi tiết"])
    
    with t1:
        st.info("💡 Mẹo: Nhập không dấu, không viết hoa. Ví dụ: 'nguyen van a 1990'")
        q = st.text_input("Nhập từ khóa:", placeholder="vd: nguyen van a 1990")
        if q:
            df = search_data('simple', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
            else: st.warning("Không tìm thấy kết quả nào.")
            
    with t2:
        defs = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        sel = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defs)] or cols[:4] 
        with st.expander("Cấu hình cột tìm kiếm", expanded=True): s = st.multiselect("Chọn trường dữ liệu:", cols, default=sel)
        inp = {}
        if s:
            c = st.columns(4)
            for i, n in enumerate(s): inp[n] = c[i % 4].text_input(n)
        if st.button("🔍 Tìm kiếm ngay"):
            v = {k: val for k, val in inp.items() if val.strip()}
            if v:
                df = search_data('manual', v)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else: st.warning("Không tìm thấy kết quả phù hợp.")
            else: st.warning("Vui lòng nhập ít nhất một thông tin.")

def main():
    # Khởi tạo state và check data
    if 'page' not in st.session_state: st.session_state['page'] = 'search'
    
    render_zalo_widget()
    
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    
    # Sidebar menu
    with st.sidebar:
        st.title("🏥 BHXH Tiện Ích")
        st.divider()
        
        if st.button("🔍 Tra cứu CSDL", use_container_width=True): st.session_state['page'] = 'search'
        if st.button("🧮 Tính BHXH Tự Nguyện", use_container_width=True): st.session_state['page'] = 'calc'
        # Nút mới
        if st.button("👵 Tính Tuổi Nghỉ Hưu", use_container_width=True): st.session_state['page'] = 'retirement'

    # Router
    p = st.session_state['page']
    
    if p == 'search': 
        cols = get_display_columns()
        if not cols: st.error("❌ Không tìm thấy dữ liệu cột."); return
        render_search(cols)
    elif p == 'calc': render_calculator()
    elif p == 'retirement': render_retirement_calculator()

if __name__ == '__main__':
    main()
