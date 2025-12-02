import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import time
import os
import zipfile
import glob
from google.cloud import firestore
from google.oauth2 import service_account
import json

# ==============================================================================
# ⚙️ CẤU HÌNH ỨNG DỤNG
# ==============================================================================
st.set_page_config(
    page_title="Hệ thống BHXH Chuyên Nghiệp",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CÁC HẰNG SỐ CẤU HÌNH
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

# ==============================================================================
# 1. KẾT NỐI DATABASE ĐÁM MÂY (FIREBASE - USER & LOGS)
# ==============================================================================
@st.cache_resource
def get_firestore_db():
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return firestore.Client(credentials=creds, project=key_dict["project_id"])
    except Exception as e:
        st.error(f"Lỗi kết nối Firestore: {e}")
        return None
    return None

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- QUẢN LÝ USER ---
def create_user(username, password, role):
    db = get_firestore_db()
    if not db: return False
    doc_ref = db.collection("users").document(username)
    if doc_ref.get().exists: return False 
    doc_ref.set({"password": make_hashes(password), "role": role, "created_at": datetime.datetime.now()})
    return True

def verify_login(username, password):
    db = get_firestore_db()
    if not db: return None
    doc = db.collection("users").document(username).get()
    if doc.exists and doc.to_dict()["password"] == make_hashes(password): return doc.to_dict()["role"]
    return None

def delete_user_cloud(username):
    db = get_firestore_db()
    if db: db.collection("users").document(username).delete(); return True
    return False

def update_password(username, new_password):
    db = get_firestore_db()
    if db:
        try: db.collection("users").document(username).update({"password": make_hashes(new_password)}); return True
        except: return False
    return False

def get_all_users():
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try: return pd.DataFrame([{"username": d.id, **d.to_dict()} for d in db.collection("users").stream()])
    except: return pd.DataFrame()

# --- QUẢN LÝ LOGS (NÂNG CẤP) ---
def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            vn_timezone = datetime.timezone(datetime.timedelta(hours=7))
            now_vn = datetime.datetime.now(vn_timezone)
            # Lưu timestamp dạng chuỗi để hiển thị và dạng server_time để sort
            db.collection("logs").add({
                "timestamp": now_vn.strftime("%Y-%m-%d %H:%M:%S"),
                "date_only": now_vn.strftime("%Y-%m-%d"), # Thêm field này để lọc theo ngày dễ hơn nếu cần
                "sort_time": firestore.SERVER_TIMESTAMP,
                "username": username, 
                "action": action, 
                "details": str(details)
            })
    except: pass

def get_logs_advanced(limit=500):
    """Lấy logs với số lượng lớn để lọc tại client"""
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try:
        # Lấy dữ liệu thô từ Firestore
        logs_ref = db.collection("logs").order_by("sort_time", direction=firestore.Query.DESCENDING).limit(limit)
        docs = logs_ref.stream()
        
        data = []
        for d in docs:
            item = d.to_dict()
            item['id'] = d.id # Lưu ID để dùng cho chức năng xóa
            data.append(item)
            
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        # Đảm bảo cột hiển thị đúng
        return df[['id', 'timestamp', 'username', 'action', 'details']]
    except Exception as e:
        st.error(f"Lỗi tải logs: {e}")
        return pd.DataFrame()

def delete_log_batch(log_ids):
    """Xóa nhiều dòng log cùng lúc"""
    db = get_firestore_db()
    if not db: return False
    try:
        batch = db.batch()
        count = 0
        deleted_count = 0
        
        for doc_id in log_ids:
            doc_ref = db.collection("logs").document(doc_id)
            batch.delete(doc_ref)
            count += 1
            deleted_count += 1
            
            # Firestore batch giới hạn 500 operations
            if count >= 400:
                batch.commit()
                batch = db.batch()
                count = 0
                
        if count > 0:
            batch.commit()
        return deleted_count
    except Exception as e:
        st.error(f"Lỗi xóa logs: {e}")
        return 0

def init_cloud_admin():
    if "admin_checked" not in st.session_state:
        # Tạo admin mặc định nếu chưa có (để tránh bị lock out)
        if verify_login("admin", "admin123") is None: 
            create_user("admin", "admin123", "admin")
        st.session_state["admin_checked"] = True

def render_zalo_widget():
    st.markdown(f"""<style>.z{{position:fixed;bottom:20px;right:20px;width:60px;height:60px;z-index:9999;animation:s 3s infinite}}@keyframes s{{0%,100%{{transform:rotate(0deg)}}10%,30%{{transform:rotate(10deg)}}20%,40%{{transform:rotate(-10deg)}}}}</style><a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%"></a>""", unsafe_allow_html=True)

# ==============================================================================
# 2. XỬ LÝ DỮ LIỆU (SQLITE - GIỮ NGUYÊN ĐỂ KHÔNG LỖI)
# ==============================================================================
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
    return False, "⚠️ Thiếu dữ liệu (File Excel hoặc ZIP)"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel. Quá trình này có thể mất vài phút.")
    conn = init_data_db(); msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang xử lý Excel..."); df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); bar.progress(40)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        
        # Tạo cột index tìm kiếm tổng hợp
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        
        # Tạo cột index cho từng trường
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
        all_cols = [r[1] for r in c.fetchall()]
        # Lọc bỏ các cột kỹ thuật (idx_)
        return [c for c in all_cols if not c.startswith('idx_') and c != 'master_search_idx' and 'kcb' not in c.lower() and c != 'index']
    except: return []
    finally: conn.close()

# --- TÌM KIẾM DỮ LIỆU ---
def search_data(q_dict):
    conn = init_data_db(); cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        conds, vals = [], []
        for c, v in q_dict.items():
            if v.strip():
                # Tìm kiếm chính xác trên cột đã được index (idx_)
                conds.append(f'idx_{unidecode.unidecode(c).strip().replace(" ", "_").lower()} LIKE ?')
                vals.append(f'%{clean_text(v)}%')
        
        if not conds: return pd.DataFrame()
        
        query = f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 100'
        return pd.read_sql_query(query, conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# ==============================================================================
# 3. CÔNG CỤ TÍNH TOÁN (BHXH TỰ NGUYỆN)
# ==============================================================================
def format_vnd(value):
    return f"{int(value):,} VNĐ".replace(",", ".")

def render_calculator():
    st.subheader("🧮 Tính Mức Đóng BHXH Tự Nguyện")
    st.caption("Công cụ ước tính số tiền đóng BHXH tự nguyện theo quy định mới nhất (2025).")

    # 1. Nhập mức thu nhập
    st.markdown("#### 1. Chọn mức thu nhập làm căn cứ đóng")
    col_inp, col_info = st.columns([2, 1])
    
    with col_inp:
        income = st.slider(
            "Mức thu nhập (kéo thanh trượt):", 
            min_value=CHUAN_NGHEO, 
            max_value=MAX_MUC_DONG, 
            value=CHUAN_NGHEO,
            step=50000,
            format="%d"
        )
        st.info(f"Mức thu nhập bạn chọn: **{format_vnd(income)}**")
        
        exact_income = st.number_input("Hoặc nhập số chính xác:", min_value=CHUAN_NGHEO, max_value=MAX_MUC_DONG, value=income, step=1000)
        if exact_income != income: income = exact_income

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
    st.markdown("#### 2. Chọn đối tượng ưu tiên")
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
    st.success(f"💡 **Kết luận:** Với mức thu nhập **{format_vnd(income)}**, bạn chỉ cần đóng **{format_vnd(so_tien_thuc_dong)}/tháng**.")

# ==============================================================================
# 4. GIAO DIỆN NGƯỜI DÙNG (UI)
# ==============================================================================
def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống</h2>", unsafe_allow_html=True)
    if not get_firestore_db(): st.error("❌ Lỗi kết nối Database Cloud. Vui lòng kiểm tra file secrets."); return
    render_zalo_widget()
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                r = verify_login(u, p)
                if r: 
                    st.session_state.update({'logged_in': True, 'username': u, 'role': r})
                    log_action(u, "Login", "Success")
                    st.rerun()
                else: 
                    st.error("Sai thông tin đăng nhập")

def render_change_password():
    st.subheader("🔒 Đổi Mật Khẩu")
    with st.form("change_pass"):
        o = st.text_input("Mật khẩu cũ", type="password")
        n = st.text_input("Mật khẩu mới", type="password")
        c = st.text_input("Nhập lại mật khẩu mới", type="password")
        if st.form_submit_button("Xác nhận đổi"):
            u = st.session_state['username']
            if verify_login(u, o):
                if n == c and len(n) >= 6:
                    if update_password(u, n): 
                        st.success("Đổi mật khẩu thành công!"); 
                        log_action(u, "ChangePass", "Success")
                        time.sleep(1)
                        st.session_state['logged_in'] = False
                        st.rerun()
                    else: st.error("Lỗi kết nối mạng")
                else: st.warning("Mật khẩu mới không khớp hoặc quá ngắn")
            else: st.error("Mật khẩu cũ không đúng")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Thông Tin")
    
    # Chỉ giữ lại phần tra cứu thủ công chính xác
    defs = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
    sel = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defs)] or cols[:4]
    
    with st.expander("🛠️ Cấu hình cột tìm kiếm", expanded=True): 
        s = st.multiselect("Chọn các trường thông tin để nhập liệu:", cols, default=sel)
    
    inp = {}
    if s:
        # Tạo lưới nhập liệu
        c = st.columns(4)
        for i, n in enumerate(s): 
            inp[n] = c[i % 4].text_input(f"Nhập {n}")
    
    c_btn, c_res = st.columns([1, 5])
    with c_btn:
        search_btn = st.button("🔎 TÌM KIẾM", type="primary", use_container_width=True)
    
    if search_btn:
        v = {k: val for k, val in inp.items() if val.strip()}
        if v:
            log_action(st.session_state['username'], "Search", str(v))
            with st.spinner("Đang truy xuất dữ liệu..."):
                df = search_data(v)
            
            if not df.empty:
                st.success(f"✅ Tìm thấy {len(df)} kết quả")
                # --- TÍNH NĂNG MỚI: TẢI KẾT QUẢ TÌM KIẾM ---
                csv = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 Tải kết quả (CSV)",
                    data=csv,
                    file_name=f"search_results_{int(time.time())}.csv",
                    mime="text/csv",
                )
                # ---------------------------------------------
                st.dataframe(df, use_container_width=True, hide_index=True)
            else: 
                st.warning("⚠️ Không tìm thấy kết quả nào phù hợp.")
        else: 
            st.warning("⚠️ Vui lòng nhập ít nhất một trường thông tin.")

def render_admin():
    st.header("🛠️ Quản Trị Hệ Thống")
    t1, t2 = st.tabs(["👤 Quản lý User", "📜 Nhật ký & Thống kê"])
    
    # --- TAB 1: USER ---
    with t1:
        st.dataframe(get_all_users(), use_container_width=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            with st.popover("➕ Thêm User"):
                with st.form("add_user"):
                    u = st.text_input("Username")
                    p = st.text_input("Password")
                    r = st.selectbox("Role", ["user", "admin"])
                    if st.form_submit_button("Tạo User"):
                        if create_user(u, p, r): 
                            st.success("Đã tạo!"); log_action(st.session_state['username'], "Add User", u); time.sleep(1); st.rerun()
                        else: st.error("User đã tồn tại")
        with c2:
            with st.popover("🗑️ Xóa User"):
                d = st.text_input("Nhập Username cần xóa:")
                if st.button("Xóa vĩnh viễn"):
                    if d != "admin" and delete_user_cloud(d): 
                        st.success("Đã xóa!"); log_action(st.session_state['username'], "Del User", d); time.sleep(1); st.rerun()
                    else: st.error("Không thể xóa admin hoặc lỗi")
        with c3:
            with st.popover("🔄 Reset Password"):
                rs = st.text_input("Username cần reset:")
                if st.button("Đặt lại thành 123456"):
                    if update_password(rs, "123456"): 
                        st.success("Đã reset!"); log_action(st.session_state['username'], "Reset Pass", rs)
                    else: st.error("Lỗi")

    # --- TAB 2: LOGS (NÂNG CẤP + THỐNG KÊ) ---
    with t2:
        # 1. Fetch dữ liệu thô
        df_logs = get_logs_advanced(limit=1000) # Lấy 1000 logs gần nhất
        
        if not df_logs.empty:
            # Convert timestamp sang datetime để lọc
            df_logs['dt'] = pd.to_datetime(df_logs['timestamp'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            
            # --- PHẦN THỐNG KÊ (DASHBOARD) ---
            st.subheader("📊 Thống Kê Hoạt Động")
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                st.caption("Top User Tích Cực Nhất")
                user_counts = df_logs['username'].value_counts().head(5)
                st.bar_chart(user_counts, color="#FF4B4B")
                
            with col_chart2:
                st.caption("Xu Hướng Hoạt Động (Theo Ngày)")
                daily_counts = df_logs['dt'].dt.date.value_counts().sort_index()
                st.line_chart(daily_counts, color="#4BFF4B")
            
            st.divider()
            # ---------------------------------

            st.subheader("📋 Bộ lọc Nhật ký")
            
            # Cột lọc
            col_d1, col_d2, col_u, col_a = st.columns(4)
            
            # Lọc theo ngày
            today = datetime.date.today()
            start_d = col_d1.date_input("Từ ngày", today - datetime.timedelta(days=7))
            end_d = col_d2.date_input("Đến ngày", today)
            
            # Lọc theo User
            users = ["Tất cả"] + list(df_logs['username'].unique())
            sel_user = col_u.selectbox("Người dùng", users)
            
            # Lọc theo Hành động
            actions = ["Tất cả"] + list(df_logs['action'].unique())
            sel_action = col_a.selectbox("Hành động", actions)
            
            # Áp dụng bộ lọc
            mask = (df_logs['dt'].dt.date >= start_d) & (df_logs['dt'].dt.date <= end_d)
            if sel_user != "Tất cả":
                mask = mask & (df_logs['username'] == sel_user)
            if sel_action != "Tất cả":
                mask = mask & (df_logs['action'] == sel_action)
                
            filtered_df = df_logs.loc[mask]
            
            # Hiển thị & Download
            c_info, c_down = st.columns([3, 1])
            c_info.info(f"Hiển thị {len(filtered_df)} dòng nhật ký.")
            
            # --- TÍNH NĂNG MỚI: DOWNLOAD LOGS ---
            csv_logs = filtered_df[['timestamp', 'username', 'action', 'details']].to_csv(index=False).encode('utf-8-sig')
            c_down.download_button(
                label="📥 Tải Logs (CSV)",
                data=csv_logs,
                file_name=f"logs_export_{today}.csv",
                mime="text/csv",
                use_container_width=True
            )
            # ------------------------------------

            # Checkbox chọn để xóa
            event = st.dataframe(
                filtered_df[['timestamp', 'username', 'action', 'details']],
                use_container_width=True,
                selection_mode="multi-row",
                on_select="rerun"
            )
            
            # Chức năng xóa
            st.divider()
            col_del_1, col_del_2 = st.columns(2)
            
            # Xóa các dòng đang hiển thị (theo bộ lọc)
            with col_del_1:
                if st.button(f"🗑️ Xóa toàn bộ {len(filtered_df)} dòng đang lọc", type="primary"):
                    if len(filtered_df) > 0:
                        count = delete_log_batch(filtered_df['id'].tolist())
                        st.success(f"Đã xóa {count} dòng nhật ký.")
                        log_action(st.session_state['username'], "Clear Logs", f"Deleted {count} entries")
                        time.sleep(1)
                        st.rerun()
            
            # Xóa dòng đang chọn trong bảng
            with col_del_2:
                selected_indices = event.selection.rows
                if selected_indices:
                    ids_to_delete = filtered_df.iloc[selected_indices]['id'].tolist()
                    if st.button(f"🗑️ Xóa {len(ids_to_delete)} dòng đã chọn"):
                        count = delete_log_batch(ids_to_delete)
                        st.success(f"Đã xóa {count} dòng.")
                        time.sleep(1)
                        st.rerun()

        else:
            st.info("Chưa có nhật ký hoạt động nào.")

# ==============================================================================
# MAIN FUNCTION
# ==============================================================================
def main():
    init_cloud_admin()
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'search'})
    render_zalo_widget()
    
    # Kiểm tra dữ liệu
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return
    
    if not st.session_state['logged_in']: 
        render_login()
    else:
        # SIDEBAR MỚI (Đã bỏ AI/Chatbot/Content)
        with st.sidebar:
            st.title(f"👤 {st.session_state['username']}")
            st.markdown("---")
            
            if st.button("🔍 Tra cứu dữ liệu", use_container_width=True): st.session_state['page'] = 'search'
            if st.button("🧮 Tính BHXH", use_container_width=True): st.session_state['page'] = 'calc'
            
            st.markdown("---")
            if st.button("🔒 Đổi mật khẩu", use_container_width=True): st.session_state['page'] = 'pass'
            
            if st.session_state['role'] == 'admin':
                st.markdown("---")
                if st.button("🛠️ Quản trị hệ thống", use_container_width=True, type="primary"): st.session_state['page'] = 'admin'
            
            st.markdown("---")
            if st.button("🚪 Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout", "User initiated")
                st.session_state['logged_in'] = False
                st.rerun()
        
        # ROUTING
        p = st.session_state['page']
        cols = get_display_columns()
        
        if p == 'search': render_search(cols)
        elif p == 'calc': render_calculator()
        elif p == 'pass': render_change_password()
        elif p == 'admin': render_admin()

if __name__ == '__main__':
    main()
