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
import pytz

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Cổng Thông Tin BHXH Số",
    page_icon="🇻🇳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 1. CẤU HÌNH & HẰNG SỐ
# ==============================================================================
ZALO_PHONE_NUMBER = "0986053006" 

# BHXH Tự Nguyện 2025
CHUAN_NGHEO = 1500000 
LUONG_CO_SO = 2340000 
MAX_MUC_DONG = 20 * LUONG_CO_SO 
TY_LE_DONG = 0.22 
HO_TRO_NGHEO = 0.50     
HO_TRO_CAN_NGHEO = 0.40 
HO_TRO_DAN_TOC = 0.30   
HO_TRO_KHAC = 0.20      

# File dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB_LOCAL = 'users_local.db'

# ==============================================================================
# 2. KẾT NỐI DATABASE (CLOUD & LOCAL)
# ==============================================================================
try:
    from google.cloud import firestore
    from google.oauth2 import service_account
    CLOUD_AVAILABLE = True
except ImportError:
    CLOUD_AVAILABLE = False

@st.cache_resource
def get_firestore_db():
    if not CLOUD_AVAILABLE: return None
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = service_account.Credentials.from_service_account_info(key_dict)
            return firestore.Client(credentials=creds, project=key_dict["project_id"])
    except: return None
    return None

def init_local_db():
    conn = sqlite3.connect(USER_DB_LOCAL, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# ==============================================================================
# 3. QUẢN LÝ USER & LOGS
# ==============================================================================
def create_user(username, password, role):
    db = get_firestore_db()
    if db: # Cloud
        doc_ref = db.collection("users").document(username)
        if doc_ref.get().exists: return False 
        doc_ref.set({"password": make_hashes(password), "role": role, "created_at": datetime.datetime.now()})
        return True
    else: # Local
        try:
            conn = init_local_db()
            conn.execute("INSERT INTO users VALUES (?, ?, ?)", (username, make_hashes(password), role))
            conn.commit(); conn.close()
            return True
        except: return False

def verify_login(username, password):
    db = get_firestore_db()
    if db: # Cloud
        doc = db.collection("users").document(username).get()
        if doc.exists and doc.to_dict()["password"] == make_hashes(password):
            return doc.to_dict()["role"]
    
    # Local
    conn = init_local_db()
    res = conn.execute("SELECT role FROM users WHERE username=? AND password=?", (username, make_hashes(password))).fetchone()
    conn.close()
    if res: return res[0]
    return None

def update_password(username, new_password):
    db = get_firestore_db()
    success = False
    if db: 
        try: 
            db.collection("users").document(username).update({"password": make_hashes(new_password)})
            success = True
        except: pass
    try:
        conn = init_local_db()
        conn.execute("UPDATE users SET password=? WHERE username=?", (make_hashes(new_password), username))
        conn.commit(); conn.close()
        success = True
    except: pass
    return success

def delete_user(username):
    db = get_firestore_db()
    if db: db.collection("users").document(username).delete()
    conn = init_local_db()
    conn.execute("DELETE FROM users WHERE username=?", (username,))
    conn.commit(); conn.close()
    return True

def get_all_users():
    data = []
    db = get_firestore_db()
    if db:
        try: 
            docs = db.collection("users").stream()
            for doc in docs: data.append({"source": "Cloud", "username": doc.id, **doc.to_dict()})
        except: pass
    if not data:
        conn = init_local_db()
        df = pd.read_sql("SELECT * FROM users", conn)
        conn.close()
        if not df.empty:
            df['source'] = 'Local'
            return df
    return pd.DataFrame(data)

# --- LOGS SYSTEM ---
def get_vn_time():
    return datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))

def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            now_vn = get_vn_time()
            db.collection("logs").add({
                "timestamp": now_vn.strftime("%Y-%m-%d %H:%M:%S"),
                "date": now_vn.strftime("%Y-%m-%d"), 
                "sort_time": firestore.SERVER_TIMESTAMP,
                "username": username, "action": action, "details": str(details)
            })
    except: pass

def get_logs(limit=2000):
    db = get_firestore_db()
    if not db: return pd.DataFrame()
    try:
        logs_ref = db.collection("logs").order_by("sort_time", direction=firestore.Query.DESCENDING).limit(limit)
        data = []
        for doc in logs_ref.stream():
            d = doc.to_dict()
            data.append({
                "id": doc.id,
                "Thời gian": d.get("timestamp"),
                "Ngày": d.get("date", d.get("timestamp", "")[:10]),
                "User": d.get("username"),
                "Hành động": d.get("action"),
                "Chi tiết": d.get("details")
            })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

def delete_all_logs():
    db = get_firestore_db()
    if not db: return False
    try:
        docs = db.collection("logs").limit(500).stream()
        count = 0
        for doc in docs:
            doc.reference.delete()
            count += 1
        return count
    except: return 0

def init_admin_account():
    if "admin_checked" not in st.session_state:
        if verify_login("admin", "admin123") is None:
            create_user("admin", "admin123", "admin")
        st.session_state["admin_checked"] = True

# ==============================================================================
# 4. XỬ LÝ DỮ LIỆU
# ==============================================================================
def clean(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_data():
    # Kiểm tra DB đã tồn tại và có dữ liệu chưa
    if os.path.exists(DB_FILE):
        try: 
            conn=init_data_db()
            res = conn.execute("SELECT count(*) FROM bhxh").fetchone()
            conn.close()
            if res and res[0] > 0: return True, "Dữ liệu sẵn sàng"
        except: 
            os.remove(DB_FILE) # File lỗi thì xóa đi làm lại
    
    # Nếu chưa có DB, thử nối file zip
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Đang ghép nối {len(parts)} phần dữ liệu...")
        try:
            with open("bhxh_full.zip", 'wb') as outfile:
                for part in parts: 
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            
            msg.info("📦 Đang giải nén dữ liệu...")
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z: z.extractall()
            
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            msg.empty()
            return True, "Đã khôi phục dữ liệu thành công!"
        except Exception as e: return False, f"Lỗi ghép file: {str(e)}"
    
    # Nếu không có zip, thử tìm file Excel gốc
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    
    return False, "⚠️ Không tìm thấy dữ liệu. Vui lòng upload file 'bhxh_data.zip.001'..."

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp từ file Excel (Sẽ chậm hơn dùng file Zip).")
    conn = init_data_db()
    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang đọc file Excel...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(30)
        
        # Chuẩn hóa tên cột
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        
        # Tạo cột tìm kiếm tổng hợp
        df['idx'] = df.apply(lambda x: clean(' '.join(x.values)), axis=1)
        
        # Tạo các cột chỉ mục phụ để tìm chính xác
        for col in df.columns: 
            if col != 'idx': df[f'i_{col}'] = df[col].apply(clean)
            
        bar.progress(80)
        msg.info("💾 Đang lưu vào Database...")
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        
        msg.info("⚡ Đang tạo chỉ mục tìm kiếm...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_m ON bhxh (idx)")
        
        bar.progress(100); msg.empty(); bar.empty(); conn.close()
        return True, "Dữ liệu đã nạp xong!"
    except Exception as e: 
        conn.close()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, f"Lỗi nạp dữ liệu: {str(e)}"

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(bhxh)")
        # Lấy tên cột, loại bỏ các cột chỉ mục (bắt đầu bằng i_ hoặc là idx)
        return [r[1] for r in c.fetchall() if not r[1].startswith('i_') and r[1] not in ['idx','index'] and 'kcb' not in r[1]]
    except: return []
    finally: conn.close()

def search(mode, q):
    conn = init_data_db()
    cols = get_display_columns()
    if not cols: return pd.DataFrame()
    
    # Chọn các cột hiển thị (loại bỏ cột ẩn)
    sel = ", ".join([f'"{c}"' for c in cols])
    
    try:
        if mode == 'ai': 
            # Tìm kiếm thông minh trên cột idx
            key = clean(q)
            if not key: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE idx LIKE ? LIMIT 100', conn, params=(f'%{key}%',))
        else:
            # Tìm kiếm chính xác từng trường
            conds, vals = [], []
            for k, v in q.items():
                if v and v.strip():
                    conds.append(f'i_{clean(k)} LIKE ?')
                    vals.append(f'%{clean(v)}%')
            
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 100', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# ==============================================================================
# 5. GIAO DIỆN (FUNCTIONS)
# ==============================================================================
def inject_custom_css():
    st.markdown("""
    <style>
        /* Hình nền Logo mờ */
        .stApp {
            background-color: #f0f8ff;
            background-image: url("https://upload.wikimedia.org/wikipedia/vi/thumb/9/93/Logo_BHXH_Vi%E1%BB%87t_Nam.svg/1200px-Logo_BHXH_Vi%E1%BB%87t_Nam.svg.png");
            background-repeat: no-repeat;
            background-position: center center;
            background-size: 40%;
            background-blend-mode: overlay;
        }
        .stApp::before {
            content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(255, 255, 255, 0.9); z-index: -1;
        }
        /* Card hiệu ứng kính */
        div[data-testid="stVerticalBlock"] > div {
            background: rgba(255, 255, 255, 0.85);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            border: 1px solid rgba(255,255,255,0.6);
            padding: 20px;
            box-shadow: 0 4px 15px rgba(0,84,166,0.1);
        }
        /* Màu chữ xanh BHXH */
        h1, h2, h3 { color: #0054a6 !important; }
        /* Sidebar */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0054a6 0%, #003366 100%);
        }
        section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] span, section[data-testid="stSidebar"] div {
            color: white !important;
        }
        /* Nút bấm */
        .stButton>button {
            background: linear-gradient(90deg, #0054a6, #0078d4);
            color: white; border: none; border-radius: 8px;
        }
        /* Widget Zalo Rung */
        .z-widget {
            position: fixed; bottom: 20px; right: 20px; width: 60px; height: 60px; z-index: 9999;
            animation: shake 3s infinite;
        }
        @keyframes shake {
            0% { transform: rotate(0deg); } 5% { transform: rotate(10deg); }
            10% { transform: rotate(-10deg); } 15% { transform: rotate(10deg); }
            20% { transform: rotate(0deg); } 100% { transform: rotate(0deg); }
        }
    </style>
    """, unsafe_allow_html=True)

def render_zalo_widget():
    st.markdown(f"""
    <a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" class="z-widget">
        <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="100%">
    </a>
    """, unsafe_allow_html=True)

def render_login():
    st.markdown("<br><br><h1 style='text-align: center;'>🔐 ĐĂNG NHẬP HỆ THỐNG</h1>", unsafe_allow_html=True)
    
    status_ph = st.empty()
    if not get_firestore_db(): 
        status_ph.warning("⚠️ Đang chạy chế độ Offline (Dữ liệu lưu trên máy).")
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                role = verify_login(u, p)
                if role:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': role})
                    log_action(u, "Login", "Thành công")
                    st.rerun()
                else:
                    st.error("Sai tên đăng nhập hoặc mật khẩu")

def render_calculator():
    st.markdown("## 🧮 Tính Mức Đóng BHXH Tự Nguyện (Luật 2025)")
    
    c1, c2 = st.columns([2, 1])
    with c1:
        income = st.slider("Chọn mức thu nhập làm căn cứ đóng:", CHUAN_NGHEO, MAX_MUC_DONG, CHUAN_NGHEO, 50000, format="%d")
        exact = st.number_input("Hoặc nhập số tiền cụ thể:", CHUAN_NGHEO, MAX_MUC_DONG, income, 1000)
        if exact != income: income = exact
    
    with c2:
        st.info(f"""
        **Thông số 2025:**
        - Chuẩn nghèo: {int(CHUAN_NGHEO):,}
        - Mức đóng: 22%
        """)
    
    st.write("---")
    dt = st.radio("Chọn đối tượng ưu tiên:", ["Khác (Hỗ trợ 20%)", "Hộ nghèo (Hỗ trợ 50%)", "Hộ cận nghèo (Hỗ trợ 40%)", "Dân tộc thiểu số (Hỗ trợ 30%)"], horizontal=True)
    
    # Tính toán
    base_fee = income * TY_LE_DONG
    
    if "nghèo" in dt and "cận" not in dt: 
        support = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_NGHEO
        label = "50%"
    elif "cận" in dt: 
        support = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_CAN_NGHEO
        label = "40%"
    elif "Dân tộc" in dt: 
        support = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_DAN_TOC
        label = "30%"
    else: 
        support = CHUAN_NGHEO * TY_LE_DONG * HO_TRO_KHAC
        label = "20%"
        
    final_fee = base_fee - support
    
    # Bảng kết quả
    st.markdown(f"### 📊 Bảng Chi Tiết Số Tiền Phải Đóng (Hỗ trợ: {label})")
    
    data = []
    modes = [("Hằng tháng", 1), ("3 tháng", 3), ("6 tháng", 6), ("12 tháng", 12)]
    
    for name, months in modes:
        total_raw = base_fee * months
        total_support = support * months
        total_pay = final_fee * months
        data.append({
            "Phương thức": name,
            "Tổng mức đóng": f"{int(total_raw):,} VNĐ",
            "Nhà nước hỗ trợ": f"{int(total_support):,} VNĐ",
            "BẠN PHẢI ĐÓNG": f"{int(total_pay):,} VNĐ"
        })
        
    df_res = pd.DataFrame(data)
    st.dataframe(df_res.style.highlight_max(axis=0, subset=["BẠN PHẢI ĐÓNG"], color='#e6ffe6'), use_container_width=True, hide_index=True)

def page_search(cols):
    st.markdown("## 🔍 Tra Cứu Dữ Liệu")
    
    t1, t2 = st.tabs(["Tra cứu Nhanh", "Tra cứu Chính xác"])
    
    with t1:
        st.caption("Nhập bất kỳ thông tin nào: Tên, Số thẻ, Năm sinh, Địa chỉ...")
        q = st.text_input("Từ khóa tìm kiếm:", placeholder="Ví dụ: nguyen van a 1990")
        if st.button("🔍 Tìm kiếm", type="primary", key="btn_search_ai"):
            if q:
                log_action(st.session_state['username'], "Search", q)
                df = search('ai', q)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả.")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.warning("Không tìm thấy kết quả nào.")
            else:
                st.warning("Vui lòng nhập từ khóa.")

    with t2:
        default_cols = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        # Lọc cột tồn tại
        valid_defaults = [c for c in default_cols if any(x in clean(c) for x in cols)] or cols[:4]
        
        selected_cols = st.multiselect("Chọn cột cần tìm:", cols, default=valid_defaults)
        
        inputs = {}
        if selected_cols:
            num_cols = 4
            rows = [selected_cols[i:i+num_cols] for i in range(0, len(selected_cols), num_cols)]
            for row in rows:
                c_cols = st.columns(len(row))
                for idx, col_name in enumerate(row):
                    inputs[col_name] = c_cols[idx].text_input(f"Nhập {col_name}")
        
        if st.button("🔍 Tìm chính xác", type="primary", key="btn_search_manual"):
            valid_inputs = {k: v for k, v in inputs.items() if v.strip()}
            if valid_inputs:
                log_action(st.session_state['username'], "Search Manual", str(valid_inputs))
                df = search('manual', valid_inputs)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả.")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.warning("Không tìm thấy kết quả.")
            else:
                st.warning("Vui lòng nhập ít nhất một thông tin.")

def page_pass():
    st.markdown("## 🔒 Đổi Mật Khẩu")
    with st.form("change_pass_form"):
        old = st.text_input("Mật khẩu cũ", type="password")
        new = st.text_input("Mật khẩu mới", type="password")
        confirm = st.text_input("Nhập lại mật khẩu mới", type="password")
        
        if st.form_submit_button("Xác nhận đổi"):
            u = st.session_state['username']
            if verify_login(u, old):
                if new == confirm and len(new) >= 6:
                    if update_password(u, new):
                        st.success("Đổi mật khẩu thành công! Vui lòng đăng nhập lại.")
                        log_action(u, "Change Pass", "Success")
                        time.sleep(1)
                        st.session_state['logged_in'] = False
                        st.rerun()
                    else:
                        st.error("Lỗi hệ thống khi cập nhật.")
                else:
                    st.warning("Mật khẩu mới không khớp hoặc quá ngắn (<6 ký tự).")
            else:
                st.error("Mật khẩu cũ không đúng.")

def page_admin():
    st.markdown("## 🛠️ Quản Trị Hệ Thống")
    
    t1, t2, t3 = st.tabs(["👥 Quản lý Người dùng", "📊 Báo cáo Thống kê", "📜 Nhật ký Hoạt động"])
    
    with t1:
        st.write("### Danh sách tài khoản")
        st.dataframe(get_all_users(), use_container_width=True)
        
        c1, c2, c3 = st.columns(3)
        with c1:
            with st.popover("➕ Thêm User Mới"):
                with st.form("add_user"):
                    u = st.text_input("Username")
                    p = st.text_input("Password")
                    r = st.selectbox("Role", ["user", "admin"])
                    if st.form_submit_button("Tạo User"):
                        if create_user(u, p, r):
                            st.success(f"Đã tạo user {u}")
                            log_action(st.session_state['username'], "Add User", u)
                            time.sleep(1); st.rerun()
                        else:
                            st.error("Tên đăng nhập đã tồn tại")
                            
        with c2:
            with st.popover("🗑️ Xóa User"):
                u_del = st.text_input("Nhập username cần xóa:")
                if st.button("Xác nhận Xóa"):
                    if u_del != "admin" and delete_user(u_del):
                        st.success(f"Đã xóa {u_del}")
                        log_action(st.session_state['username'], "Delete User", u_del)
                        time.sleep(1); st.rerun()
                    else:
                        st.error("Không thể xóa admin hoặc user không tồn tại")
                        
        with c3:
            with st.popover("🔄 Reset Mật khẩu"):
                u_rs = st.text_input("Username cần reset:")
                if st.button("Reset về '123456'"):
                    if update_password(u_rs, "123456"):
                        st.success("Đã reset thành công")
                        log_action(st.session_state['username'], "Reset Pass", u_rs)
                    else:
                        st.error("Lỗi khi reset")

    with t2:
        st.write("### Báo cáo chi tiết")
        logs = get_logs(2000)
        if not logs.empty:
            # Thống kê đăng nhập
            st.markdown("#### 1. Thống kê Đăng nhập")
            login_data = logs[logs['Hành động'] == 'Login']
            if not login_data.empty:
                # Biểu đồ theo ngày
                daily_login = login_data.groupby('Ngày').size().reset_index(name='Lượt')
                st.bar_chart(daily_login.set_index('Ngày'))
                
                # Bảng chi tiết
                st.write("Chi tiết lượt đăng nhập theo User:")
                user_login = login_data.groupby(['Ngày', 'User']).size().reset_index(name='Số lần')
                st.dataframe(user_login, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu đăng nhập.")
                
            st.divider()
            
            # Thống kê tra cứu
            st.markdown("#### 2. Lịch sử Tra cứu Chi tiết")
            search_data = logs[logs['Hành động'].str.contains('Search', case=False, na=False)]
            
            if not search_data.empty:
                # Bộ lọc
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    filter_date = st.selectbox("Chọn ngày:", ["Tất cả"] + list(search_data['Ngày'].unique()))
                with col_f2:
                    filter_user = st.selectbox("Chọn nhân viên:", ["Tất cả"] + list(search_data['User'].unique()))
                
                filtered = search_data
                if filter_date != "Tất cả":
                    filtered = filtered[filtered['Ngày'] == filter_date]
                if filter_user != "Tất cả":
                    filtered = filtered[filtered['User'] == filter_user]
                    
                st.write(f"Tìm thấy {len(filtered)} lượt tra cứu:")
                st.dataframe(
                    filtered[['Thời gian', 'User', 'Chi tiết']],
                    use_container_width=True,
                    column_config={"Chi tiết": st.column_config.TextColumn("Nội dung tìm kiếm", width="large")}
                )
            else:
                st.info("Chưa có dữ liệu tra cứu.")
        else:
            st.warning("Chưa có nhật ký nào.")

    with t3:
        col_l1, col_l2 = st.columns([3, 1])
        with col_l1: st.write("### Nhật ký hoạt động (Toàn bộ)")
        with col_l2:
            if st.button("🗑️ Xóa SẠCH Nhật ký", type="primary"):
                count = delete_all_logs()
                st.success(f"Đã xóa {count} dòng nhật ký.")
                time.sleep(1); st.rerun()
        
        st.dataframe(get_logs(1000), use_container_width=True)

# ==============================================================================
# MAIN FUNCTION
# ==============================================================================
def main():
    # 1. Inject CSS
    inject_custom_css()
    
    # 2. Init Admin (nếu chưa có)
    init_admin_account()
    
    # 3. Session State Init
    if 'logged_in' not in st.session_state:
        st.session_state.update({'logged_in': False, 'page': 'search'})
    
    # 4. Render Widget Zalo
    render_zalo_widget()
    
    # 5. Kiểm tra dữ liệu
    ok, msg = check_data()
    if not ok:
        st.error(msg)
        return

    # 6. Routing (Điều hướng)
    if not st.session_state['logged_in']:
        render_login()
    else:
        # Sidebar Menu
        with st.sidebar:
            st.image("https://upload.wikimedia.org/wikipedia/vi/9/93/Logo_BHXH_Vi%E1%BB%87t_Nam.svg", width=120)
            st.markdown(f"### Xin chào, {st.session_state['username']}!")
            st.markdown("---")
            
            if st.button("🔍 Tra cứu Dữ liệu", use_container_width=True): 
                st.session_state['page'] = 'search'
            
            if st.button("🧮 Tính BHXH Tự nguyện", use_container_width=True): 
                st.session_state['page'] = 'calc'
                
            st.markdown("---")
            
            if st.button("🔒 Đổi Mật khẩu", use_container_width=True): 
                st.session_state['page'] = 'pass'
                
            if st.session_state['role'] == 'admin':
                if st.button("🛠️ Quản trị Hệ thống", use_container_width=True): 
                    st.session_state['page'] = 'admin'
            
            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        # Main Content Area
        page = st.session_state['page']
        cols = get_display_columns()
        
        if page == 'search':
            page_search(cols)
        elif page == 'calc':
            render_calculator()
        elif page == 'pass':
            page_pass()
        elif page == 'admin':
            page_admin()

# ENTRY POINT
if __name__ == '__main__':
    main()
