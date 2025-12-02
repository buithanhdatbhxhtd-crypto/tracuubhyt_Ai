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
import shutil

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

# HẰNG SỐ TÍNH TOÁN BHXH 2025
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
# Lưu ý: Prefix phải khớp với tên file trên GitHub (bhxh_data.zip.001, .002...)
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
    if db: 
        doc_ref = db.collection("users").document(username)
        if doc_ref.get().exists: return False 
        doc_ref.set({"password": make_hashes(password), "role": role, "created_at": datetime.datetime.now()})
        return True
    try:
        conn = init_local_db()
        conn.execute("INSERT INTO users VALUES (?, ?, ?)", (username, make_hashes(password), role))
        conn.commit(); conn.close()
        return True
    except: return False

def verify_login(username, password):
    db = get_firestore_db()
    if db: 
        doc = db.collection("users").document(username).get()
        if doc.exists and doc.to_dict()["password"] == make_hashes(password):
            return doc.to_dict()["role"]
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

def log_action(username, action, details=""):
    try:
        db = get_firestore_db()
        if db:
            now_vn = datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
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
                "id": doc.id, "Thời gian": d.get("timestamp"), "Ngày": d.get("date", ""),
                "User": d.get("username"), "Hành động": d.get("action"), "Chi tiết": d.get("details")
            })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

def delete_all_logs():
    db = get_firestore_db()
    if not db: return 0
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
# 4. XỬ LÝ DỮ LIỆU THÔNG MINH (SMART SEARCH)
# ==============================================================================
# Hàm làm sạch: Bỏ dấu, chữ thường, BỎ KHOẢNG TRẮNG -> Tìm kiếm bất chấp
def clean_smart(text): 
    if pd.isna(text): return ""
    # 1. Chuyển thành chuỗi, bỏ dấu
    text = unidecode.unidecode(str(text)).lower()
    # 2. Bỏ khoảng trắng và ký tự lạ
    text = text.replace(' ', '').replace('-', '').replace('.', '').strip()
    return text

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_data():
    if os.path.exists(DB_FILE):
        try: 
            conn=init_data_db()
            # Kiểm tra xem bảng đã có các cột index chưa (để đảm bảo data cũ tương thích code mới)
            cols_info = pd.read_sql("PRAGMA table_info(bhxh)", conn)
            col_names = [c[1] for c in cols_info.values]
            conn.close()
            
            # Nếu thiếu cột index quan trọng, coi như data lỗi, cần nạp lại
            if 'idx_master' not in col_names:
                return False, "⚠️ Database phiên bản cũ. Cần nạp lại dữ liệu."
                
            return True, "Dữ liệu sẵn sàng"
        except: 
            return False, "⚠️ Lỗi Database. Đang thử phục hồi..."
    
    # Tìm file zip
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Đang nối {len(parts)} phần dữ liệu...")
        try:
            with open("bhxh_full.zip", 'wb') as o:
                for p in parts: 
                    with open(p, 'rb') as i: o.write(i.read())
            
            msg.info("📦 Đang giải nén...")
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z: z.extractall()
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            msg.empty()
            return True, "Đã khôi phục dữ liệu!"
        except Exception as e: return False, f"Lỗi file zip: {str(e)}"
    
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    
    return False, f"⚠️ Không tìm thấy file dữ liệu. Hãy upload file '{ZIP_PART_PREFIX}001'..."

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp dữ liệu mới và tạo chỉ mục tìm kiếm thông minh...")
    conn = init_data_db()
    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang đọc file Excel (Quá trình này chỉ chạy 1 lần)...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(20)
        
        # Chuẩn hóa tên cột hiển thị (Giữ nguyên, chỉ viết thường)
        df.columns = [unidecode.unidecode(str(c)).strip().lower().replace(' ', '_').replace('.', '') for c in df.columns]
        
        # Xử lý dữ liệu: Chuyển tất cả về chuỗi, xóa .0 ở số
        df = df.astype(str).replace(r'\.0$', '', regex=True).replace(['nan', 'None', 'NaT'], '')
        
        msg.info("⚡ Đang tạo 'Siêu Chỉ Mục' để tìm kiếm bất chấp lỗi gõ...")
        bar.progress(40)
        
        # 1. Tạo cột idx_master: Gộp toàn bộ thông tin dòng lại, làm sạch triệt để
        df['idx_master'] = df.apply(lambda x: clean_smart(' '.join(x.values)), axis=1)
        
        # 2. Tạo cột index riêng cho từng trường quan trọng (Để tìm chính xác)
        for col in df.columns:
            if col != 'idx_master' and not col.startswith('idx_'):
                df[f'idx_{col}'] = df[col].apply(clean_smart)
        
        bar.progress(70)
        msg.info("💾 Đang lưu vào Database tối ưu hóa...")
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        
        msg.info("🚀 Đang đánh index SQL...")
        conn.execute("CREATE INDEX IF NOT EXISTS i_master ON bhxh (idx_master)")
        # Index cho các cột hay tìm
        search_cols = ['sobhxh', 'hoten', 'socmnd', 'cccd', 'ngaysinh', 'soso']
        for c in df.columns:
            if any(s in c for s in search_cols):
                try: conn.execute(f"CREATE INDEX IF NOT EXISTS i_{c} ON bhxh (idx_{c})")
                except: pass

        bar.progress(100)
        msg.success("✅ Hoàn tất! Hệ thống đã sẵn sàng.")
        time.sleep(1)
        msg.empty(); bar.empty(); conn.close()
        return True, "OK"
    except Exception as e: 
        conn.close()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, f"Lỗi nạp: {str(e)}"

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(bhxh)")
        # Chỉ lấy các cột gốc, bỏ các cột idx_
        return [r[1] for r in c.fetchall() if not r[1].startswith('idx_')]
    except: return []
    finally: conn.close()

def search_smart(mode, q_input, col_filter=None):
    conn = init_data_db()
    cols = get_display_columns()
    if not cols: return pd.DataFrame()
    
    # Lấy danh sách cột hiển thị (trừ cột idx)
    sel = ", ".join([f'"{c}"' for c in cols])
    
    try:
        # 1. Làm sạch từ khóa tìm kiếm (Bỏ dấu, bỏ cách)
        # Ví dụ: "Nguyễn Văn A" -> "nguyenvana"
        clean_q = clean_smart(q_input)
        
        if not clean_q: return pd.DataFrame()

        if mode == 'ai' or not col_filter:
            # Tìm trong cột tổng hợp idx_master
            # Dùng LIKE %key%
            sql = f'SELECT {sel} FROM bhxh WHERE idx_master LIKE ? LIMIT 100'
            return pd.read_sql_query(sql, conn, params=(f'%{clean_q}%',))
        
        else:
            # Tìm chính xác trong các cột được chọn
            # Ví dụ: idx_hoten LIKE %nguyenvana%
            conds = []
            params = []
            for col_name, val in col_filter.items():
                val_clean = clean_smart(val)
                if val_clean:
                    # Tìm trong cột index tương ứng (idx_hoten, idx_sobhxh...)
                    conds.append(f'idx_{col_name} LIKE ?')
                    params.append(f'%{val_clean}%')
            
            if not conds: return pd.DataFrame()
            
            sql = f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 100'
            return pd.read_sql_query(sql, conn, params=tuple(params))
            
    except Exception as e:
        # st.error(f"Lỗi tìm kiếm: {e}") 
        return pd.DataFrame()
    finally: conn.close()

# ==============================================================================
# 5. GIAO DIỆN
# ==============================================================================
def inject_custom_css():
    st.markdown("""
    <style>
        .stApp { background-color: #f0f8ff; }
        section[data-testid="stSidebar"] { background: linear-gradient(180deg, #0054a6 0%, #003366 100%); }
        section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] span, section[data-testid="stSidebar"] div { color: white !important; }
        .stTextInput input { border-radius: 5px; border: 1px solid #ccc; }
        .stButton button { background-color: #0054a6; color: white; border-radius: 5px; width: 100%; }
        .stButton button:hover { background-color: #004080; }
        .stDataFrame { border: 1px solid #ddd; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

def render_login():
    st.markdown("<br><br><h1 style='text-align: center; color: #0054a6;'>🔐 ĐĂNG NHẬP HỆ THỐNG</h1>", unsafe_allow_html=True)
    if not get_firestore_db(): st.warning("⚠️ Chế độ Offline.")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("User"); p = st.text_input("Pass", type='password')
            if st.form_submit_button("Login", use_container_width=True):
                r = verify_login(u, p)
                if r:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': r})
                    log_action(u, "Login", "Success")
                    st.rerun()
                else: st.error("Sai thông tin")

def render_calculator():
    st.markdown("## 🧮 Tính BHXH Tự Nguyện (2025)")
    c1, c2 = st.columns([2, 1])
    with c1:
        inc = st.slider("Thu nhập:", CHUAN_NGHEO, MAX_MUC_DONG, CHUAN_NGHEO, 50000, format="%d")
        exc = st.number_input("Nhập số:", CHUAN_NGHEO, MAX_MUC_DONG, inc, 1000)
        if exc != inc: inc = exc
    with c2: st.info(f"Chuẩn nghèo: {int(CHUAN_NGHEO):,}\nTỷ lệ: 22%")
    dt = st.radio("Đối tượng:", ["Khác (20%)", "Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Dân tộc (30%)"], horizontal=True)
    base = inc * TY_LE_DONG
    if "nghèo" in dt: supp=base*HO_TRO_NGHEO; l="50%"
    elif "cận" in dt: supp=base*HO_TRO_CAN_NGHEO; l="40%"
    elif "Dân tộc" in dt: supp=base*HO_TRO_DAN_TOC; l="30%"
    else: supp=base*HO_TRO_KHAC; l="20%"
    final = base - supp
    st.write(f"### Bảng Đóng (Hỗ trợ {l})")
    data = [{"Kỳ": k, "Phải Đóng": f"{int(final*m):,}"} for k, m in [("1 tháng",1), ("3 tháng",3), ("6 tháng",6), ("12 tháng",12)]]
    st.table(pd.DataFrame(data))

def page_search(cols):
    st.markdown("## 🔍 Tra Cứu Thông Tin")
    
    # Tab 1: Tìm nhanh (Gõ gì cũng tìm)
    # Tab 2: Tìm chính xác (Chọn cột)
    t1, t2 = st.tabs(["⚡ Tìm Nhanh", "🎯 Tìm Chi Tiết"])
    
    with t1:
        st.caption("Gõ bất cứ gì: Tên, Số thẻ, Năm sinh... (Không cần dấu, không cần viết hoa, không cần cách)")
        q = st.text_input("Từ khóa:", placeholder="vd: nguyen van a 1990 hoặc 6714001414")
        if q:
            # Gọi hàm tìm kiếm thông minh
            df = search_smart('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả.")
                st.dataframe(df, use_container_width=True)
                log_action(st.session_state['username'], "Search Fast", q)
            else:
                st.warning("Không tìm thấy.")
                
    with t2:
        # Sắp xếp cột ưu tiên
        prio = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        srt = sorted(cols, key=lambda x: (x not in prio, x))
        sel_cols = st.multiselect("Chọn cột:", srt, default=[c for c in srt if c in prio][:3])
        
        inputs = {}
        if sel_cols:
            c_ui = st.columns(len(sel_cols))
            for i, c_name in enumerate(sel_cols):
                inputs[c_name] = c_ui[i].text_input(f"Nhập {c_name}")
            
            if st.button("Tìm kiếm"):
                valid = {k: v for k, v in inputs.items() if v.strip()}
                if valid:
                    df = search_smart('manual', '', valid)
                    if not df.empty:
                        st.success(f"Thấy {len(df)} kết quả.")
                        st.dataframe(df, use_container_width=True)
                        log_action(st.session_state['username'], "Search Detail", str(valid))
                    else: st.warning("Không thấy.")
                else: st.error("Nhập ít nhất 1 ô.")

def page_admin():
    st.markdown("## 🛠️ Quản Trị")
    t1, t2 = st.tabs(["User", "Logs"])
    with t1:
        st.dataframe(get_all_users(), use_container_width=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            with st.popover("➕ Thêm User"):
                with st.form("add"):
                    u=st.text_input("User"); p=st.text_input("Pass"); r=st.selectbox("Role",["user","admin"])
                    if st.form_submit_button("Tạo"):
                        if create_user(u,p,r): st.success("OK"); time.sleep(1); st.rerun()
                        else: st.error("Trùng")
        with c2:
            with st.popover("🗑️ Xóa User"):
                d=st.text_input("User xóa:")
                if st.button("Xóa"):
                    if d!="admin" and delete_user(d): st.success("OK"); time.sleep(1); st.rerun()
        with c3:
            with st.popover("🔄 Reset Pass"):
                rs=st.text_input("User reset:")
                if st.button("Về 123456"):
                    if update_password(rs,"123456"): st.success("OK")
    with t2:
        logs = get_logs(2000)
        if not logs.empty:
            st.bar_chart(logs[logs['Hành động']=='Login'].groupby('Ngày').size())
            if st.button("🗑️ Xóa Logs"): delete_all_logs(); st.rerun()
            st.dataframe(logs, use_container_width=True)

def main():
    inject_custom_css()
    init_admin_account()
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in':False,'page':'search'})
    
    # Nút reset dữ liệu khẩn cấp (ẩn dưới cùng sidebar)
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/vi/9/93/Logo_BHXH_Vi%E1%BB%87t_Nam.svg", width=100)
        st.markdown(f"### Xin chào, {st.session_state.get('username','')}")
        
        if st.session_state['logged_in']:
            if st.button("🔍 Tra cứu"): st.session_state['page']='search'
            if st.button("🧮 Tính BHXH"): st.session_state['page']='calc'
            if st.session_state.get('role')=='admin':
                if st.button("🛠️ Quản trị"): st.session_state['page']='admin'
            
            st.markdown("---")
            if st.button("Đăng xuất"):
                log_action(st.session_state['username'],"Logout"); st.session_state['logged_in']=False; st.rerun()
            
            # Nút reset data
            st.markdown("---")
            if st.button("🗑️ Xóa Data & Nạp lại"):
                if os.path.exists(DB_FILE): os.remove(DB_FILE)
                st.success("Đã xóa DB. Vui lòng refresh trang."); time.sleep(2); st.rerun()

    ok, msg = check_data()
    if not ok: 
        if "Cần nạp lại" in msg:
            # Tự động nạp lại nếu DB cũ
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()
        st.error(msg)
        return

    if not st.session_state['logged_in']: render_login()
    else:
        p = st.session_state['page']
        cols = get_display_columns()
        if p=='search': page_search(cols)
        elif p=='calc': render_calculator()
        elif p=='admin': page_admin()

if __name__ == '__main__':
    main()
