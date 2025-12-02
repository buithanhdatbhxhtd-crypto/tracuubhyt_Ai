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

# HẰNG SỐ TÍNH TOÁN BHXH 2025
CHUAN_NGHEO = 1500000 
LUONG_CO_SO = 2340000 
MAX_MUC_DONG = 20 * LUONG_CO_SO 
TY_LE_DONG = 0.22 
HO_TRO_NGHEO = 0.50     
HO_TRO_CAN_NGHEO = 0.40 
HO_TRO_DAN_TOC = 0.30   
HO_TRO_KHAC = 0.20      

# Tên file dữ liệu
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
    if db: 
        try: 
            db.collection("users").document(username).update({"password": make_hashes(new_password)})
            return True
        except: pass
    try:
        conn = init_local_db()
        conn.execute("UPDATE users SET password=? WHERE username=?", (make_hashes(new_password), username))
        conn.commit(); conn.close()
        return True
    except: pass
    return False

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
# 4. XỬ LÝ DỮ LIỆU (DEBUG MODE)
# ==============================================================================
def clean(text): return unidecode.unidecode(str(text)).lower().strip() if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_data():
    # DEBUG: In ra thư mục hiện tại để kiểm tra file
    st.write(f"📂 Thư mục hiện tại: `{os.getcwd()}`")
    files = os.listdir('.')
    # st.write(f"📂 Danh sách file: {files}") # Bật dòng này nếu cần xem hết file
    
    # Kiểm tra DB đã có dữ liệu chưa
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db()
            # Kiểm tra số lượng dòng
            count = conn.execute("SELECT count(*) FROM bhxh").fetchone()[0]
            
            # Kiểm tra xem có bảng bhxh không
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
            
            conn.close()
            
            if count > 0:
                return True, f"✅ Dữ liệu sẵn sàng! (Tổng: {count} dòng)"
            else:
                return False, "⚠️ Database trống rỗng."
        except Exception as e:
            st.error(f"Lỗi đọc DB: {e}")
            os.remove(DB_FILE) # Xóa file lỗi

    # Nếu chưa có DB, thử tìm file zip
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Tìm thấy {len(parts)} phần dữ liệu nén: {parts}")
        
        try:
            # Nối file
            with open("bhxh_full.zip", 'wb') as outfile:
                for part in parts:
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            
            msg.info("📦 Đang giải nén...")
            
            # Giải nén và kiểm tra file bên trong
            with zipfile.ZipFile("bhxh_full.zip", 'r') as z:
                file_list = z.namelist()
                msg.info(f"📄 File trong zip: {file_list}")
                z.extractall()
                
                # Kiểm tra xem file Excel có được giải nén đúng không
                if EXCEL_FILE not in file_list and not os.path.exists(EXCEL_FILE):
                     return False, f"❌ Lỗi: Trong file zip không có file tên là '{EXCEL_FILE}'"
            
            if os.path.exists("bhxh_full.zip"): os.remove("bhxh_full.zip")
            
            # Sau khi giải nén xong, nạp vào SQLite ngay
            return import_excel_to_sqlite()
            
        except Exception as e:
            return False, f"❌ Lỗi xử lý file nén: {str(e)}"

    # Nếu có file Excel gốc
    if os.path.exists(EXCEL_FILE):
        return import_excel_to_sqlite()
    
    return False, f"❌ Không tìm thấy dữ liệu. Cần file '{EXCEL_FILE}' hoặc '{ZIP_PART_PREFIX}*'"

def import_excel_to_sqlite():
    status = st.empty()
    bar = st.progress(0)
    status.info("⏳ Đang nạp dữ liệu từ Excel vào Database... Vui lòng không tắt tab.")
    
    conn = init_data_db()
    try:
        # Đọc Excel
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        
        # Chuẩn hóa tên cột: Xóa dấu, chữ thường, thay khoảng trắng bằng _
        # Ví dụ: "Số BHXH" -> "so_bhxh"
        df.columns = [unidecode.unidecode(str(c)).strip().lower().replace(' ', '_').replace('.', '') for c in df.columns]
        
        # Chuyển tất cả dữ liệu sang string để tìm kiếm dễ hơn
        df = df.astype(str)
        
        # Xóa các dòng trống
        df.replace(['nan', 'None', 'NaT'], '', inplace=True)
        
        bar.progress(50)
        status.info(f"Đang lưu {len(df)} dòng vào SQLite...")
        
        # Lưu vào SQLite
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=10000)
        
        # Tạo chỉ mục cho các cột quan trọng để tìm nhanh
        # Tự động tìm các cột tiềm năng
        cols = df.columns.tolist()
        important_cols = [c for c in cols if any(x in c for x in ['so', 'ma', 'ten', 'date', 'ngay'])]
        
        for col in important_cols:
            try:
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{col} ON bhxh ({col})")
            except: pass

        bar.progress(100)
        status.success("✅ Nạp dữ liệu thành công!")
        time.sleep(1)
        status.empty()
        bar.empty()
        conn.close()
        return True, "OK"
        
    except Exception as e:
        conn.close()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, f"❌ Lỗi nạp dữ liệu: {str(e)}"

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(bhxh)")
        # Lấy tất cả tên cột
        return [r[1] for r in c.fetchall()]
    except: return []
    finally: conn.close()

def search_exact(conditions):
    conn = init_data_db()
    cols = get_display_columns()
    if not cols: return pd.DataFrame()
    
    # Xây dựng câu truy vấn động
    query = "SELECT * FROM bhxh WHERE "
    params = []
    
    clauses = []
    for col, val in conditions.items():
        # Dùng LIKE để tìm kiếm linh hoạt hơn (không cần chính xác 100% chữ hoa/thường)
        # Nhưng vẫn đảm bảo nội dung phải khớp
        clauses.append(f'"{col}" LIKE ?')
        params.append(f"%{val.strip()}%") # Thêm % để tìm kiếm tương đối nếu cần
    
    if not clauses: return pd.DataFrame()
    
    query += " AND ".join(clauses)
    query += " LIMIT 100" # Giới hạn kết quả để tránh treo máy
    
    try:
        # Debug: In câu truy vấn ra để kiểm tra
        # st.write(f"Query: {query}")
        # st.write(f"Params: {params}")
        return pd.read_sql_query(query, conn, params=tuple(params))
    except Exception as e:
        st.error(f"Lỗi truy vấn: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# ==============================================================================
# 5. GIAO DIỆN
# ==============================================================================
def inject_custom_css():
    st.markdown("""
    <style>
        .stApp { background-color: #f0f8ff; }
        /* Sidebar đẹp */
        section[data-testid="stSidebar"] { background: linear-gradient(180deg, #0054a6 0%, #003366 100%); }
        section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] span, section[data-testid="stSidebar"] div { color: white !important; }
        
        /* Form nhập liệu đẹp */
        .stTextInput input { border-radius: 5px; border: 1px solid #ccc; }
        .stButton button { background-color: #0054a6; color: white; border-radius: 5px; width: 100%; }
        .stButton button:hover { background-color: #004080; }
        
        /* Bảng kết quả */
        .stDataFrame { border: 1px solid #ddd; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

def render_zalo_widget():
    st.markdown(f"""
    <a href="https://zalo.me/{ZALO_PHONE_NUMBER}" target="_blank" style="position:fixed; bottom:20px; right:20px; z-index:9999;">
        <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Icon_of_Zalo.svg/1200px-Icon_of_Zalo.svg.png" width="50">
    </a>
    """, unsafe_allow_html=True)

def render_login():
    st.markdown("<br><br><h1 style='text-align: center; color: #0054a6;'>🔐 ĐĂNG NHẬP HỆ THỐNG</h1>", unsafe_allow_html=True)
    
    if not get_firestore_db(): st.warning("⚠️ Chế độ Offline (Dữ liệu user lưu trên máy).")
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                r = verify_login(u, p)
                if r:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': r})
                    log_action(u, "Login", "Thành công")
                    st.rerun()
                else:
                    st.error("Sai thông tin đăng nhập")

def render_calculator():
    st.markdown("## 🧮 Tính Mức Đóng BHXH Tự Nguyện (Luật 2025)")
    c1, c2 = st.columns([2, 1])
    with c1:
        inc = st.slider("Thu nhập làm căn cứ đóng:", CHUAN_NGHEO, MAX_MUC_DONG, CHUAN_NGHEO, 50000, format="%d")
        exc = st.number_input("Hoặc nhập số:", CHUAN_NGHEO, MAX_MUC_DONG, inc, 1000)
        if exc != inc: inc = exc
    
    with c2:
        st.info(f"Chuẩn nghèo: {int(CHUAN_NGHEO):,}\nTỷ lệ đóng: 22%")
    
    dt = st.radio("Đối tượng:", ["Khác (20%)", "Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Dân tộc (30%)"], horizontal=True)
    
    base = inc * TY_LE_DONG
    if "nghèo" in dt and "cận" not in dt: supp=base*HO_TRO_NGHEO; l="50%"
    elif "cận" in dt: supp=base*HO_TRO_CAN_NGHEO; l="40%"
    elif "Dân tộc" in dt: supp=base*HO_TRO_DAN_TOC; l="30%"
    else: supp=base*HO_TRO_KHAC; l="20%"
    
    final = base - supp
    
    st.markdown(f"#### 📊 Bảng Chi Tiết (Hỗ trợ: {l})")
    data = [{"Kỳ hạn": l, "Tổng": f"{int(base*m):,}", "Hỗ trợ": f"{int(supp*m):,}", "PHẢI ĐÓNG": f"{int(final*m):,}"} for l, m in [("1 tháng", 1), ("3 tháng", 3), ("6 tháng", 6), ("12 tháng", 12)]]
    st.dataframe(pd.DataFrame(data), use_container_width=True)

def page_search(cols):
    st.markdown("## 🔍 Tra Cứu Dữ Liệu")
    
    # Hiển thị danh sách cột để user biết cần nhập gì
    # st.write(f"Các cột có trong dữ liệu: {', '.join(cols)}")
    
    with st.expander("ℹ️ Hướng dẫn", expanded=True):
        st.info("Nhập chính xác thông tin vào các ô bên dưới để tìm kiếm. Ví dụ: Nhập số BHXH vào ô 'sobhxh' hoặc 'soso'.")

    # Tự động tạo form nhập liệu dựa trên tên cột
    # Ưu tiên các cột quan trọng lên đầu
    priority_cols = ['sobhxh', 'soso', 'hoten', 'ngaysinh', 'socmnd', 'cccd', 'mabenhnhan']
    sorted_cols = sorted(cols, key=lambda x: (x not in priority_cols, x))
    
    # Chọn cột hiển thị
    selected_cols = st.multiselect("Chọn trường thông tin muốn tìm:", sorted_cols, default=[c for c in sorted_cols if c in priority_cols][:4])
    
    inputs = {}
    if selected_cols:
        with st.form("search_form"):
            # Chia layout 4 cột
            num_cols = 4
            rows = [selected_cols[i:i+num_cols] for i in range(0, len(selected_cols), num_cols)]
            
            for row in rows:
                c_cols = st.columns(num_cols)
                for idx, col_name in enumerate(row):
                    if idx < len(c_cols):
                        with c_cols[idx]:
                            inputs[col_name] = st.text_input(f"Nhập {col_name}", key=f"in_{col_name}")
            
            submitted = st.form_submit_button("🔍 Tìm kiếm ngay", type="primary")
            
            if submitted:
                # Lọc các ô có dữ liệu
                valid_conditions = {k: v for k, v in inputs.items() if v.strip()}
                
                if valid_conditions:
                    with st.spinner("Đang tìm kiếm..."):
                        log_action(st.session_state['username'], "Search", str(valid_conditions))
                        df = search_exact(valid_conditions)
                        
                        if not df.empty:
                            st.success(f"🎉 Tìm thấy {len(df)} kết quả!")
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.warning("Không tìm thấy kết quả nào khớp với thông tin đã nhập.")
                            st.write("Gợi ý: Hãy thử nhập ít thông tin hơn hoặc kiểm tra lại chính tả.")
                else:
                    st.warning("Vui lòng nhập ít nhất một thông tin để tìm kiếm.")

def page_pass():
    st.markdown("## 🔒 Đổi Mật Khẩu")
    with st.form("p"):
        o=st.text_input("Mật khẩu cũ",type='password')
        n=st.text_input("Mật khẩu mới",type='password')
        c=st.text_input("Nhập lại",type='password')
        if st.form_submit_button("Xác nhận"):
            u=st.session_state['username']
            if verify_login(u,o):
                if n==c and len(n)>=6:
                    if update_password(u,n): st.success("Thành công! Đăng nhập lại."); time.sleep(1); st.session_state['logged_in']=False; st.rerun()
                    else: st.error("Lỗi hệ thống")
                else: st.warning("Mật khẩu ngắn/không khớp")
            else: st.error("Mật khẩu cũ sai")

def page_admin():
    st.markdown("## 🛠️ Quản Trị Hệ Thống")
    t1, t2 = st.tabs(["Người dùng", "Nhật ký & Thống kê"])
    
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
                rs=st.text_input("User reset (123456):")
                if st.button("Reset"):
                    if update_password(rs,"123456"): st.success("OK")
    
    with t2:
        logs = get_logs(2000)
        if not logs.empty:
            st.write("### Thống kê Đăng nhập")
            login_data = logs[logs['Hành động'] == 'Login']
            if not login_data.empty:
                st.bar_chart(login_data.groupby('Ngày').size())
            
            st.write("### Nhật ký hoạt động")
            if st.button("🗑️ Xóa SẠCH Nhật ký", type="primary"):
                delete_all_logs()
                st.rerun()
            st.dataframe(logs, use_container_width=True)
        else:
            st.info("Chưa có nhật ký.")

def main():
    inject_custom_css()
    init_admin_account()
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in':False,'page':'search'})
    render_zalo_widget()
    
    # Kiểm tra và nạp dữ liệu
    ok, msg = check_data()
    if not ok: 
        st.error(msg)
        # Thêm nút reset DB nếu lỗi
        if st.button("Thử nạp lại dữ liệu"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()
        return
    
    if not st.session_state['logged_in']: 
        render_login()
    else:
        with st.sidebar:
            st.image("https://upload.wikimedia.org/wikipedia/vi/9/93/Logo_BHXH_Vi%E1%BB%87t_Nam.svg", width=100)
            st.markdown(f"### Xin chào, {st.session_state['username']}!")
            if st.button("🔍 Tra cứu",use_container_width=True): st.session_state['page']='search'
            if st.button("🧮 Tính BHXH",use_container_width=True): st.session_state['page']='calc'
            if st.button("🔒 Đổi Mật khẩu",use_container_width=True): st.session_state['page']='pass'
            if st.session_state['role']=='admin':
                if st.button("🛠️ Quản trị",use_container_width=True): st.session_state['page']='admin'
            st.markdown("---")
            if st.button("Đăng xuất",use_container_width=True):
                log_action(st.session_state['username'],"Logout"); st.session_state['logged_in']=False; st.rerun()
        
        p=st.session_state['page']; cols=get_display_columns()
        if p=='search': page_search(cols)
        elif p=='calc': render_calculator()
        elif p=='pass': page_pass()
        elif p=='admin': page_admin()

if __name__ == '__main__':
    main()
