import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import google.generativeai as genai
import time
import os

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Big Data",
    page_icon="🗃️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tên file
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
USER_DB = 'users.db'

# --- 1. CÁC HÀM XỬ LÝ USER & LOG ---
def init_user_db():
    """Khởi tạo DB User. Nếu lỗi schema cũ sẽ tự động backup và tạo mới."""
    conn = sqlite3.connect(USER_DB, check_same_thread=False)
    c = conn.cursor()
    
    # Tạo bảng users
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    
    # Tạo bảng logs (Thử tạo, nếu lỗi do bảng cũ thiếu cột thì sẽ drop tạo lại)
    try:
        c.execute("SELECT * FROM logs LIMIT 1")
        cols = [description[0] for description in c.description]
        if "details" not in cols: # Kiểm tra xem có cột details không
            raise Exception("Old schema")
    except:
        # Nếu bảng chưa có hoặc schema cũ, tạo mới
        c.execute("DROP TABLE IF EXISTS logs")
        c.execute('''CREATE TABLE logs
                     (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    # Admin mặc định
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", 
                  ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def log_action(username, action, details=""):
    """Ghi nhật ký hoạt động"""
    try:
        conn = init_user_db()
        c = conn.cursor()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, str(details)))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Log Error: {e}") # In lỗi ra terminal để debug nếu cần

def configure_gemini():
    api_key = None
    try: api_key = st.secrets["GOOGLE_API_KEY"]
    except: pass
    if not api_key: api_key = st.session_state.get('user_api_key', '')
    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU LỚN (SQLITE) ---

def clean_text(text):
    if pd.isna(text) or str(text).lower() == 'nan' or str(text).strip() == '': return ""
    return unidecode.unidecode(str(text)).lower().replace(' ', '')

def init_data_db():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def import_excel_to_sqlite():
    if not os.path.exists(EXCEL_FILE):
        return False, f"⚠️ Không tìm thấy file '{EXCEL_FILE}'"

    conn = init_data_db()
    cursor = conn.cursor()
    
    try:
        # Kiểm tra dữ liệu có sẵn chưa
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'")
        if cursor.fetchone()[0]:
            cursor.execute("SELECT count(*) FROM bhxh")
            if cursor.fetchone()[0] > 0:
                conn.close()
                return True, "Ready"

    except: pass

    # Nạp mới
    msg = st.empty()
    bar = st.progress(0)
    try:
        msg.info(f"⏳ Đang xử lý dữ liệu lớn từ '{EXCEL_FILE}' (Chỉ làm 1 lần)...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(30)
        
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').replace('.','').lower() for c in df.columns]
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT', '<NA>'], '', inplace=True)
        bar.progress(50)

        # Tạo cột tìm kiếm tổng hợp
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        
        # Tạo cột tìm kiếm từng trường
        for col in df.columns:
            if col != 'master_search_idx':
                df[f'idx_{col}'] = df[col].apply(clean_text)
        
        bar.progress(70)
        msg.info("💾 Đang tối ưu hóa và lưu vào Database...")
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        
        # Indexing
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        conn.commit()
        
        bar.progress(100)
        time.sleep(1)
        msg.empty(); bar.empty()
        conn.close()
        return True, "Done"
    except Exception as e:
        conn.close()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, str(e)

def get_display_columns():
    conn = init_data_db()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(bhxh)")
        return [r[1] for r in cursor.fetchall() if not r[1].startswith('idx_') and r[1] != 'master_search_idx' and r[1] != 'index']
    except: return []
    finally: conn.close()

# --- 3. TÌM KIẾM ---

def search_data(mode, query_data):
    conn = init_data_db()
    cols = get_display_columns()
    if not cols: return pd.DataFrame()
    select_sql = ", ".join([f'"{c}"' for c in cols])
    
    try:
        if mode == 'ai':
            key = clean_text(query_data)
            if not key: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {select_sql} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{key}%',))
        
        elif mode == 'manual':
            conds, vals = [], []
            for c, v in query_data.items():
                if v and v.strip():
                    # Map tên cột hiển thị sang tên cột DB (lowercase, no space)
                    db_col = unidecode.unidecode(c).strip().replace(' ', '_').lower()
                    conds.append(f'idx_{db_col} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            
            if not conds: return pd.DataFrame()
            sql = f'SELECT {select_sql} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50'
            return pd.read_sql_query(sql, conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- 4. GIAO DIỆN ---

def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type='password')
            if st.form_submit_button("Đăng nhập", use_container_width=True):
                conn = init_user_db()
                user = conn.execute('SELECT * FROM users WHERE username=? AND password=?', (u, make_hashes(p))).fetchone()
                conn.close()
                if user:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': user[2]})
                    log_action(u, "Login", "Thành công")
                    st.rerun()
                else: st.error("Sai thông tin!")

def render_search_ai(is_ready):
    st.subheader("🤖 Tra Cứu Thông Minh")
    st.caption("Nhập bất kỳ thông tin nào (Tên viết liền, số thẻ, ngày sinh...)")
    q = st.text_input("Từ khóa:", placeholder="vd: buithanhdat 24011988")
    if q:
        df = search_data('ai', q)
        if not df.empty:
            st.success(f"Tìm thấy {len(df)} kết quả.")
            st.dataframe(df, use_container_width=True)
            log_action(st.session_state['username'], "Search AI", q)
            
            if len(df) == 1 and is_ready:
                with st.expander("💡 Phân tích AI", expanded=True):
                    with st.spinner("AI đang đọc..."):
                        try:
                            model = genai.GenerativeModel('gemini-pro')
                            res = model.generate_content(f"Dữ liệu BHXH: {df.iloc[0].to_dict()}. Tóm tắt và tư vấn quyền lợi bằng tiếng Việt.")
                            st.write(res.text)
                        except: st.warning("Lỗi kết nối AI")
        else: st.warning("Không tìm thấy.")

def render_search_manual(cols):
    st.subheader("📋 Tra Cứu Chính Xác")
    st.caption("Nhập thông tin vào các ô tương ứng (không cần dấu).")
    
    with st.expander("Chọn trường tìm kiếm", expanded=True):
        default = [c for c in cols if any(x in c for x in ['ten', 'sinh', 'ma', 'so'])]
        selected = st.multiselect("Cột:", cols, default=default or cols[:3])
    
    # FIX LỖI BẬC THANG Ở ĐÂY
    inputs = {}
    if selected:
        # Tạo danh sách cột trên giao diện 1 lần duy nhất
        ui_cols = st.columns(len(selected))
        for i, col_name in enumerate(selected):
            # Gán mỗi input vào đúng cột của nó
            inputs[col_name] = ui_cols[i].text_input(f"Nhập {col_name}")
    
    if st.button("🔍 Tìm kiếm", type="primary"):
        df = search_data('manual', inputs)
        if not df.empty:
            st.success(f"Tìm thấy {len(df)} kết quả.")
            st.dataframe(df, use_container_width=True)
            log_action(st.session_state['username'], "Search Manual", str(inputs))
        else: st.warning("Không tìm thấy.")

def render_admin():
    st.header("🛠️ Quản Trị Hệ Thống")
    conn = init_user_db()
    t1, t2 = st.tabs(["👥 Quản lý User", "📜 Nhật ký hoạt động"])
    
    with t1:
        col_list, col_action = st.columns([2, 1])
        with col_list:
            st.subheader("Danh sách tài khoản")
            users_df = pd.read_sql("SELECT username, role FROM users", conn)
            st.dataframe(users_df, use_container_width=True)
        
        with col_action:
            st.subheader("Thao tác")
            action_type = st.radio("Chọn hành động:", ["Thêm User", "Xóa User"])
            
            if action_type == "Thêm User":
                with st.form("add_u"):
                    u = st.text_input("Username")
                    p = st.text_input("Password", type="password")
                    r = st.selectbox("Role", ["user", "admin"])
                    if st.form_submit_button("Tạo mới", use_container_width=True):
                        try:
                            conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r))
                            conn.commit()
                            st.success(f"Đã tạo {u}")
                            log_action(st.session_state['username'], "Add User", u)
                            time.sleep(1); st.rerun()
                        except: st.error("Tên đã tồn tại!")
            
            else: # Xóa User
                users = [u for u in users_df['username'] if u != 'admin'] # Không cho xóa admin gốc
                if users:
                    u_del = st.selectbox("Chọn user cần xóa", users)
                    if st.button("🗑️ Xóa vĩnh viễn", type="primary", use_container_width=True):
                        conn.execute("DELETE FROM users WHERE username=?", (u_del,))
                        conn.commit()
                        st.success(f"Đã xóa {u_del}")
                        log_action(st.session_state['username'], "Delete User", u_del)
                        time.sleep(1); st.rerun()
                else:
                    st.info("Không có user phụ để xóa.")

    with t2:
        c1, c2 = st.columns([3, 1])
        with c1: st.subheader("Lịch sử")
        with c2:
            if st.button("🗑️ Xóa tất cả Logs"):
                conn.execute("DELETE FROM logs")
                conn.commit()
                st.rerun()
        
        # Nút sửa lỗi DB nếu logs không hiện
        if st.checkbox("⚠️ Gặp lỗi không hiện Logs?"):
            if st.button("Sửa lỗi Database ngay"):
                conn.execute("DROP TABLE IF EXISTS logs")
                conn.execute("CREATE TABLE logs (timestamp TEXT, username TEXT, action TEXT, details TEXT)")
                conn.commit()
                st.success("Đã đặt lại bảng Logs. Hãy thử tra cứu lại để kiểm tra.")
                time.sleep(1); st.rerun()

        logs = pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100", conn)
        st.dataframe(logs, use_container_width=True)
    
    conn.close()

# --- MAIN ---
def main():
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'ai'})
    
    ok, msg = import_excel_to_sqlite()
    if not ok:
        st.error(msg)
        if st.button("Thử lại"): 
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()
        return

    if not st.session_state['logged_in']:
        render_login()
    else:
        with st.sidebar:
            st.title(f"Xin chào, {st.session_state['username']}")
            with st.popover("🔑 Cấu hình AI Key"):
                k = st.text_input("Google API Key", type="password", value=st.session_state.get('user_api_key',''))
                if k: st.session_state['user_api_key'] = k
            
            st.divider()
            
            if st.button("🤖 Tra cứu AI", use_container_width=True): st.session_state['page'] = 'ai'
            if st.button("📋 Tra cứu Thủ công", use_container_width=True): st.session_state['page'] = 'manual'
            
            if st.session_state['role'] == 'admin':
                st.divider()
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        cols = get_display_columns()
        is_ai = configure_gemini()
        p = st.session_state['page']
        
        if p == 'ai': render_search_ai(is_ai)
        elif p == 'manual': render_search_manual(cols)
        elif p == 'admin': render_admin()

if __name__ == '__main__':
    init_user_db()
    main()
