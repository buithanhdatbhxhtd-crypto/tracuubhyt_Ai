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
    """Khởi tạo DB User + Kích hoạt WAL Mode cho hiệu suất cao"""
    conn = sqlite3.connect(USER_DB, check_same_thread=False)
    # Kích hoạt chế độ WAL để xử lý nhiều người dùng cùng lúc
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    
    try:
        c.execute("SELECT * FROM logs LIMIT 1")
        cols = [d[0] for d in c.description]
        if "details" not in cols: raise Exception()
    except:
        c.execute("DROP TABLE IF EXISTS logs")
        c.execute('''CREATE TABLE logs
                     (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", 
                  ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def log_action(username, action, details=""):
    try:
        conn = init_user_db()
        c = conn.cursor()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, str(details)))
        conn.commit()
        conn.close()
    except: pass

def configure_gemini():
    api_key = None
    try: api_key = st.secrets["GOOGLE_API_KEY"]
    except: pass
    if not api_key: api_key = st.session_state.get('user_api_key', '')
    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU LỚN (SQLITE + WAL) ---
def clean_text(text):
    if pd.isna(text) or str(text).lower() == 'nan' or str(text).strip() == '': return ""
    return unidecode.unidecode(str(text)).lower().replace(' ', '')

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    # QUAN TRỌNG: Kích hoạt WAL để 30 người cùng đọc không bị khóa file
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def import_excel_to_sqlite():
    if not os.path.exists(EXCEL_FILE):
        return False, f"⚠️ Không tìm thấy file '{EXCEL_FILE}'"

    conn = init_data_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'")
        if cursor.fetchone()[0]:
            cursor.execute("SELECT count(*) FROM bhxh")
            if cursor.fetchone()[0] > 0:
                conn.close(); return True, "Ready"
    except: pass

    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info(f"⏳ Đang nạp dữ liệu lớn từ '{EXCEL_FILE}' (Chỉ lần đầu)...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(30)
        
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').replace('.','').lower() for c in df.columns]
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT', '<NA>'], '', inplace=True)
        bar.progress(50)

        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        for col in df.columns:
            if col != 'master_search_idx':
                df[f'idx_{col}'] = df[col].apply(clean_text)
        
        bar.progress(70)
        # Chunksize nhỏ hơn để ổn định memory
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=2000)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        
        bar.progress(100); time.sleep(0.5); msg.empty(); bar.empty()
        conn.close(); return True, "Done"
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
    # Quote tên cột để tránh lỗi SQL nếu tên cột trùng từ khóa
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
                    db_col = unidecode.unidecode(c).strip().replace(' ', '_').lower()
                    conds.append(f'idx_{db_col} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {select_sql} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
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
                res = conn.execute('SELECT * FROM users WHERE username=? AND password=?', (u, make_hashes(p))).fetchone()
                conn.close()
                if res:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': res[2]})
                    log_action(u, "Login", "Success")
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
            # hide_index=True để ẩn cột số thứ tự 0,1,2...
            st.dataframe(df, use_container_width=True, hide_index=True)
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
    
    inputs = {}
    if selected:
        # Layout ngang hàng (Sửa lỗi bậc thang)
        ui_cols = st.columns(len(selected))
        for i, col_name in enumerate(selected):
            inputs[col_name] = ui_cols[i].text_input(f"Nhập {col_name}")
    
    if st.button("🔍 Tìm kiếm", type="primary"):
        df = search_data('manual', inputs)
        if not df.empty:
            st.success(f"Tìm thấy {len(df)} kết quả.")
            # hide_index=True để bảng đẹp hơn
            st.dataframe(df, use_container_width=True, hide_index=True)
            log_action(st.session_state['username'], "Search Manual", str(inputs))
        else: st.warning("Không tìm thấy.")

def render_admin():
    st.header("🛠️ Quản Trị Hệ Thống")
    conn = init_user_db()
    t1, t2 = st.tabs(["👥 Quản lý User", "📜 Nhật ký"])
    
    with t1:
        c1, c2 = st.columns([2, 1])
        with c1:
            st.caption("Danh sách tài khoản")
            # hide_index=True cho bảng admin
            st.dataframe(pd.read_sql("SELECT username, role FROM users", conn), use_container_width=True, hide_index=True)
        
        with c2:
            act = st.radio("Thao tác:", ["Thêm", "Xóa"])
            if act == "Thêm":
                with st.form("add"):
                    u = st.text_input("User")
                    p = st.text_input("Pass", type="password")
                    r = st.selectbox("Quyền", ["user", "admin"])
                    if st.form_submit_button("Tạo"):
                        try:
                            conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r))
                            conn.commit(); st.success("Xong"); time.sleep(0.5); st.rerun()
                        except: st.error("Trùng tên")
            else:
                us = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
                if us:
                    ud = st.selectbox("Chọn User", us)
                    if st.button("Xóa"):
                        conn.execute("DELETE FROM users WHERE username=?", (ud,))
                        conn.commit(); st.success("Đã xóa"); time.sleep(0.5); st.rerun()
                else: st.info("Trống")

    with t2:
        if st.button("Xóa Logs"):
            conn.execute("DELETE FROM logs"); conn.commit(); st.rerun()
        
        if st.checkbox("⚠️ Fix lỗi Logs"):
            if st.button("Reset bảng Logs"):
                conn.execute("DROP TABLE IF EXISTS logs")
                conn.execute("CREATE TABLE logs (timestamp TEXT, username TEXT, action TEXT, details TEXT)")
                conn.commit(); st.success("Đã reset"); st.rerun()
        
        # hide_index=True cho bảng logs
        st.dataframe(pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100", conn), use_container_width=True, hide_index=True)
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
            st.title(f"Hi, {st.session_state['username']}")
            with st.popover("🔑 Cấu hình AI"):
                k = st.text_input("API Key", type="password", value=st.session_state.get('user_api_key',''))
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

