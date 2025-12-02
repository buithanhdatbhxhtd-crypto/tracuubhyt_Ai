# --- TỰ ĐỘNG NÂNG CẤP THƯ VIỆN AI NẾU CŨ (FIX LỖI 404) ---
import subprocess
import sys
try:
    import google.generativeai as genai
    import pkg_resources
    # Kiểm tra version, nếu thấp hơn 0.7.0 thì update ngay lập tức
    ver = pkg_resources.get_distribution("google-generativeai").version
    if ver < "0.7.0":
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "google-generativeai"])
        import google.generativeai as genai
except:
    pass # Bỏ qua nếu lỗi import pkg_resources

import streamlit as st
import pandas as pd
import sqlite3
import unidecode
import time
import os
import zipfile
import glob
import numpy as np # Thêm thư viện numpy cho xử lý số liệu

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Chuyên Nghiệp (Không Đăng Nhập)",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 CẤU HÌNH HỆ THỐNG
# ==============================================================================
HARDCODED_API_KEY = "AIzaSyBd6MNZdWTsJiTy1yrrWK4G2PsltqFV6eg" 
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

# Tên file
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 

# --- WIDGET ZALO ---
def render_zalo_widget():
    st.markdown(f"""<style>.z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}@keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}</style><a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%"></a>""", unsafe_allow_html=True)

# --- HỆ THỐNG AI (CƠ CHẾ MỚI) ---
def configure_ai():
    # Sử dụng API Key mặc định hoặc từ Streamlit secrets
    key = HARDCODED_API_KEY or st.secrets.get("GOOGLE_API_KEY")
    if key: genai.configure(api_key=key); return True
    return False

def get_ai_response(prompt, role_desc="", stream=False):
    """Thử lần lượt các model từ mới đến cũ để tránh lỗi 404. Giữ lại để không phá vỡ Chatbot."""
    if not configure_ai(): return "⚠️ Lỗi: Chưa có API Key."
    models_to_try = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-pro"]
    full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
    last_error = ""
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            if stream: return model.generate_content(full_prompt, stream=True)
            return model.generate_content(full_prompt).text
        except Exception as e:
            last_error = str(e)
            if "429" in last_error: return "⚠️ Hệ thống đang quá tải. Vui lòng thử lại sau 1 phút."
            continue 
    return f"⚠️ Không kết nối được AI. Lỗi cuối cùng: {last_error}"

# --- XỬ LÝ DỮ LIỆU ---
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

# Hàm mới để lấy dữ liệu cho tính năng gợi ý (Autosuggest)
@st.cache_data(ttl=3600)
def get_autocomplete_options(column_name):
    conn = init_data_db()
    try:
        # Giới hạn 200 giá trị duy nhất để tránh quá tải
        df = pd.read_sql_query(f'SELECT DISTINCT "{column_name}" FROM bhxh WHERE "{column_name}" != "" LIMIT 200', conn)
        return df[column_name].tolist()
    except:
        return []
    finally:
        conn.close()

# Hàm mới để lấy toàn bộ dữ liệu (cho mục đích thống kê, nhưng chỉ lấy các cột cần thiết)
@st.cache_data(ttl=3600)
def get_all_data_for_analytics():
    conn = init_data_db()
    # Chỉ lấy các cột có thể phân tích: hoten, ngaysinh (để tính tuổi), gioi_tinh, ...
    cols_to_fetch = ["ho_ten", "ngay_sinh", "gioi_tinh"] 
    # Điều chỉnh tên cột SQL theo tên trong DB của bạn
    cols_to_fetch_db = [c for c in cols_to_fetch if c in get_display_columns()]
    sel = ", ".join([f'"{c}"' for c in cols_to_fetch_db])
    if not sel: return pd.DataFrame()
    try:
        df = pd.read_sql_query(f'SELECT {sel} FROM bhxh', conn)
        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()

# --- TÌM KIẾM ---
def search_data(mode, q):
    conn = init_data_db(); cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        if mode == 'ai':
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

# --- TÍNH BHXH TỰ NGUYỆN (CẬP NHẬT 2025) ---
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
    # Mức đóng chuẩn (chưa trừ hỗ trợ) = Thu nhập * 22%
    muc_dong_chuan = income * TY_LE_DONG
    
    # Mức hỗ trợ của nhà nước = Chuẩn nghèo * % Hỗ trợ (theo yêu cầu mới)
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

    # Số tiền thực đóng = Mức đóng chuẩn - Mức hỗ trợ
    so_tien_thuc_dong = muc_dong_chuan - muc_ho_tro

    # 3. Hiển thị kết quả (Bảng so sánh các phương thức đóng)
    st.markdown("---")
    st.markdown(f"#### 📊 Bảng Chi Tiết Số Tiền Phải Đóng (Hỗ trợ: {tile_hotro})")
    
    # Tạo dữ liệu cho bảng
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
    
    # Highlight cột kết quả
    st.dataframe(
        df_result.style.highlight_max(axis=0, subset=["BẠN PHẢI ĐÓNG"], color='#e6ffe6'),
        use_container_width=True,
        hide_index=True
    )
    
    st.success(f"💡 **Kết luận:** Với mức thu nhập **{format_vnd(income)}**, đối tượng **{doi_tuong}**, bạn chỉ cần đóng **{format_vnd(so_tien_thuc_dong)}/tháng**.")

# --- ƯỚC TÍNH LƯƠNG HƯU (CÓ DÙNG AI) ---
def render_pension_calculator():
    st.subheader("👵👴 Ước Tính Lương Hưu")
    st.caption("Sử dụng AI và công thức BHXH Việt Nam để ước tính mức lương hưu hàng tháng.")

    col1, col2 = st.columns(2)
    
    # Giả định người dùng nhập các thông số chính
    with col1:
        years = st.number_input("Số năm đóng BHXH:", min_value=1, max_value=40, value=20, step=1)
    
    with col2:
        # Mức lương đóng bình quân có thể nằm trong khoảng Chuẩn Nghèo đến Tối Đa
        avg_income = st.number_input(f"Mức thu nhập đóng BHXH bình quân (VNĐ):", 
                                     min_value=CHUAN_NGHEO, 
                                     max_value=MAX_MUC_DONG, 
                                     value=3000000, 
                                     step=100000)

    if st.button("Tính Toán Lương Hưu Ước Tính", use_container_width=True):
        
        # --- BƯỚC 1: TÍNH TOÁN GIẢ ĐỊNH (SIMPLIFIED CALCULATION) ---
        # Giả định Tỷ lệ hưởng (Áp dụng cho Nam giới): 
        # 45% cho 20 năm đóng đầu tiên, mỗi năm sau + 2% (Công thức đơn giản, chỉ mang tính minh họa nhanh)
        
        # Nam giới: 45% cho 20 năm đầu tiên
        base_years = 20
        base_rate = 0.45 
        
        # Xác định tỷ lệ hưởng (%)
        if years <= base_years:
            percent = base_rate
        else:
            extra_years = years - base_years
            # Áp dụng 45% cho 20 năm đầu + (số năm dư * 2%)
            percent = min(base_rate + extra_years * 0.02, 0.75) # Tối đa 75%

        # Lương hưu hàng tháng (ước tính đơn giản)
        estimated_pension_raw = avg_income * percent
        
        # --- BƯỚC 2: DÙNG AI ĐỂ CUNG CẤP GIẢI THÍCH CHI TIẾT ---
        prompt = (f"Tính toán chi tiết và giải thích công thức ước tính lương hưu hàng tháng cho một người có: "
                  f"1. Tổng số năm đóng BHXH: {years} năm. "
                  f"2. Mức thu nhập đóng BHXH bình quân: {format_vnd(avg_income)}. "
                  f"Dựa trên Luật BHXH Việt Nam hiện hành (giả định người này là nam giới và nghỉ hưu đủ tuổi). "
                  f"Trình bày kết quả và giải thích rõ ràng TỶ LỆ HƯỞNG (%) được tính như thế nào.")
        
        with st.spinner("Đang tham vấn Chuyên gia AI để tính toán chi tiết và giải thích..."):
            ai_explanation = get_ai_response(prompt, "Chuyên gia tính lương hưu BHXH hàng đầu. Trả lời bằng tiếng Việt thân thiện, chính xác, và chia thành các mục rõ ràng.")

        # --- BƯỚC 3: HIỂN THỊ KẾT QUẢ ---
        st.markdown("---")
        st.subheader("💡 Kết Quả Ước Tính (Tham khảo)")
        st.info(f"💰 Mức Lương Hưu Ước Tính/tháng (giả định): **{format_vnd(estimated_pension_raw)}**")
        st.markdown(f"Tỷ lệ hưởng áp dụng: **{percent * 100:.2f}%**")
        st.markdown("---")
        st.markdown("#### 📝 Giải Thích Chi Tiết từ Chuyên Gia AI:")
        st.markdown(ai_explanation)

# --- CHỨC NĂNG THỐNG KÊ TỔNG QUAN (TÍNH NĂNG MỚI) ---
def render_analytics():
    st.subheader("📈 Thống Kê Tổng Quan Dữ Liệu")
    st.caption("Tổng hợp các chỉ số cơ bản và phân bố dữ liệu BHXH.")
    
    df = get_all_data_for_analytics()
    if df.empty:
        st.warning("⚠️ Không có dữ liệu để phân tích.")
        return

    total_records = len(df)
    st.info(f"Tổng số hồ sơ BHXH trong cơ sở dữ liệu: **{total_records:,}**".replace(",", "."))

    st.markdown("---")
    
    # 1. Thống kê theo Giới tính
    if "gioi_tinh" in df.columns:
        st.markdown("#### 1. Phân bố theo Giới tính")
        gender_counts = df["gioi_tinh"].str.upper().value_counts()
        gender_chart_data = pd.DataFrame({
            'Giới tính': gender_counts.index,
            'Số lượng': gender_counts.values
        })
        st.bar_chart(gender_chart_data, x='Giới tính', y='Số lượng')
    
    # 2. Thống kê theo Độ tuổi
    if "ngay_sinh" in df.columns:
        st.markdown("#### 2. Phân bố theo Độ tuổi")
        
        # Chuyển đổi ngày sinh sang format Date và tính tuổi
        # Thử nhiều định dạng ngày tháng nếu cần (DD/MM/YYYY, YYYY-MM-DD, ...)
        def calculate_age(dob_str):
            try:
                # Giả định định dạng ngày phổ biến (DD/MM/YYYY)
                dob = pd.to_datetime(dob_str, format='%d/%m/%Y', errors='coerce')
                if pd.isna(dob):
                    # Thử định dạng khác (YYYY-MM-DD)
                    dob = pd.to_datetime(dob_str, format='%Y-%m-%d', errors='coerce')
                
                if pd.notna(dob):
                    today = pd.to_datetime('today')
                    return (today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day)))
                return np.nan
            except:
                return np.nan

        df['Tuổi'] = df['ngay_sinh'].apply(calculate_age)
        
        # Tạo bins độ tuổi (ví dụ: 0-20, 21-30, 31-40, 41-50, 51-60, >60)
        bins = [0, 20, 30, 40, 50, 60, 150]
        labels = ['Dưới 20', '21-30', '31-40', '41-50', '51-60', 'Trên 60']
        
        # Lọc bỏ các giá trị tuổi không hợp lệ/NaN
        df_valid_age = df.dropna(subset=['Tuổi'])
        
        if not df_valid_age.empty:
            df_valid_age['Nhóm Tuổi'] = pd.cut(df_valid_age['Tuổi'], bins=bins, labels=labels, right=True)
            age_counts = df_valid_age['Nhóm Tuổi'].value_counts(sort=False).reset_index()
            age_counts.columns = ['Nhóm Tuổi', 'Số lượng']
            
            st.bar_chart(age_counts, x='Nhóm Tuổi', y='Số lượng')
        else:
            st.warning("Không thể tính toán tuổi do dữ liệu ngày sinh không hợp lệ.")


# --- GIAO DIỆN ---
def render_search(cols):
    st.subheader("🔍 Tra Cứu")
    t1, t2 = st.tabs(["Nhanh (AI)", "Chi tiết"])
    
    # TAB 1: TRA CỨU NHANH (AI)
    with t1:
        q = st.text_input("Từ khóa:", placeholder="vd: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Nút tải về kết quả tra cứu nhanh
                csv_ai = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Tải về kết quả (.csv)",
                    data=csv_ai,
                    file_name='ket_qua_tra_cuu_nhanh.csv',
                    mime='text/csv',
                    key='download_ai'
                )

                if len(df) == 1:
                    with st.expander("✨ AI Phân tích"):
                        # Giữ lại tính năng phân tích 1 hồ sơ duy nhất
                        st.write(get_ai_response(f"Dữ liệu: {df.iloc[0].to_dict()}", "Chuyên gia BHXH tóm tắt."))
            else: st.warning("Không thấy.")
    
    # TAB 2: TRA CỨU CHI TIẾT (Có Tự Động Gợi Ý)
    with t2:
        defs = ['sobhxh', 'ho_ten', 'ngay_sinh', 'so_cmnd'] # Sử dụng tên cột chuẩn hóa
        
        # Lấy các cột chính để gợi ý
        important_cols = [c for c in cols if any(x in c.lower() for x in defs)] 
        
        with st.expander("Cấu hình & Tự động Gợi ý", expanded=True): 
            s = st.multiselect("Cột:", cols, default=important_cols or cols[:4])
        
        inp = {}
        if s:
            c = st.columns(4)
            for i, n in enumerate(s): 
                # Áp dụng Tự Động Gợi Ý (Autosuggest) cho các cột chính
                if n in ["ho_ten", "so_bhxh", "so_cmnd"]:
                    options = get_autocomplete_options(n)
                    # Thêm option trống để người dùng có thể nhập thủ công
                    options.insert(0, "")
                    inp[n] = c[i % 4].selectbox(f"Chọn/Nhập {n}:", options=options)
                else:
                    # Dùng text input cho các cột khác (Ngày sinh, địa chỉ...)
                    inp[n] = c[i % 4].text_input(n) 

        if st.button("Tìm"):
            v = {k: val for k, val in inp.items() if val.strip()}
            if v:
                df = search_data('manual', v)
                if not df.empty:
                    st.success(f"Thấy {len(df)} KQ")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    
                    # Nút tải về kết quả tra cứu chi tiết
                    csv_manual = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Tải về kết quả (.csv)",
                        data=csv_manual,
                        file_name='ket_qua_tra_cuu_chi_tiet.csv',
                        mime='text/csv',
                        key='download_manual'
                    )
                else: st.warning("Không thấy.")
            else: st.warning("Nhập thông tin.")

def render_chatbot():
    st.subheader("🤖 Chatbot")
    if "msg" not in st.session_state: st.session_state.msg = [{"role": "model", "content": "Chào bạn! Tôi là chuyên gia BHXH AI. Tôi có thể giải đáp các thắc mắc về chính sách BHXH, BHYT."}]
    
    # Hiển thị lịch sử chat
    for m in st.session_state.msg: st.chat_message(m["role"]).markdown(m["content"])
    
    # Nhận input mới
    if p := st.chat_input():
        # log_action("anonymous", "Chatbot", p) # Đã loại bỏ log
        st.session_state.msg.append({"role": "user", "content": p})
        st.chat_message("user").markdown(p)
        
        with st.chat_message("model"):
            ph = st.empty(); res = ""; 
            # Dùng stream cho trải nghiệm tốt hơn
            s = get_ai_response(p, "Chuyên gia BHXH Việt Nam. Trả lời bằng tiếng Việt thân thiện, chính xác.", True)
            try:
                if isinstance(s, str): ph.markdown(s); res = s
                else:
                    for c in s: 
                        if c.text: res += c.text; ph.markdown(res + "▌")
                    ph.markdown(res)
            except: ph.markdown(res)
            st.session_state.msg.append({"role": "model", "content": res})

def render_content():
    st.subheader("✍️ Tạo Nội Dung Tuyên Truyền")
    st.caption("Sử dụng AI để viết các bài tuyên truyền về chính sách BHXH, BHYT.")
    c1, c2 = st.columns([1, 2])
    
    # Input area
    with c1:
        t = st.text_area("Chủ đề cần viết (ví dụ: Lợi ích của BHXH tự nguyện)", height=150)
        if st.button("Viết Nội Dung", use_container_width=True) and t:
            # log_action("anonymous", "Content", t) # Đã loại bỏ log
            with st.spinner("Đang tạo nội dung..."): 
                # Cập nhật session state với nội dung mới
                st.session_state['generated_txt'] = get_ai_response(f"Viết một bài tuyên truyền ngắn gọn, hấp dẫn, dễ hiểu về chủ đề: {t}", "Chuyên viên truyền thông BHXH.")
    
    # Output area
    with c2:
        result_text = st.session_state.get('generated_txt', "Kết quả sẽ hiển thị ở đây sau khi bạn nhấn 'Viết Nội Dung'.")
        st.text_area("Kết quả:", value=result_text, height=400)

def main():
    # Khởi tạo state và check data
    if 'page' not in st.session_state: st.session_state['page'] = 'search'
    
    render_zalo_widget()
    
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    
    # Giao diện không cần đăng nhập
    with st.sidebar:
        st.title("🏥 Hệ thống BHXH")
        st.caption("Chạy ở chế độ công khai (Không cần đăng nhập)")
        st.divider()
        
        # Menu chính
        if st.button("🔍 Tra cứu CSDL", use_container_width=True): st.session_state['page'] = 'search'
        if st.button("📈 Thống Kê Tổng Quan", use_container_width=True): st.session_state['page'] = 'analytics' # Thêm nút mới
        if st.button("🧮 Tính BHXH Tự Nguyện", use_container_width=True): st.session_state['page'] = 'calc'
        if st.button("👵👴 Ước Tính Lương Hưu", use_container_width=True): st.session_state['page'] = 'pension' 
        if st.button("🤖 Chatbot Hỏi Đáp", use_container_width=True): st.session_state['page'] = 'chat'
        if st.button("✍️ Tạo Nội Dung", use_container_width=True): st.session_state['page'] = 'content'

    # Hiển thị nội dung theo page
    p = st.session_state['page']
    
    if p == 'search': 
        cols = get_display_columns()
        if not cols: st.error("❌ Không tìm thấy dữ liệu cột."); return
        render_search(cols)
    elif p == 'analytics': render_analytics() # Trang thống kê mới
    elif p == 'calc': render_calculator()
    elif p == 'pension': render_pension_calculator() 
    elif p == 'chat': render_chatbot()
    elif p == 'content': render_content()

if __name__ == '__main__':
    main()
