import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import google.generativeai as genai
import time
import os
import zipfile
import glob

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(page_title="Hệ thống BHXH Big Data", page_icon="🗃️", layout="wide")
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
# Quan trọng: Tên tiền tố của các file bị cắt
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB = 'users.db'

# --- 1. HỆ THỐNG USER & LOG ---
def init_user_db():
    conn = sqlite3.connect(USER_DB, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    try:
        c.execute("SELECT * FROM logs LIMIT 1")
        cols = [d[0] for d in c.description]
        if "details" not in cols: raise Exception()
    except:
        c.execute("DROP TABLE IF EXISTS logs")
        c.execute('''CREATE TABLE logs (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users VALUES (?, ?, ?)", ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password): return hashlib.sha256(str.encode(password)).hexdigest()
def log_action(u, a, d=""): 
    try: 
        c = init_user_db()
        c.execute("INSERT INTO logs VALUES (?,?,?,?)", (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), u, a, str(d)))
        c.commit(); c.close()
    except: pass

def configure_gemini():
    key = st.secrets.get("GOOGLE_API_KEY", st.session_state.get('user_api_key', ''))
    if key: genai.configure(api_key=key); return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU (QUAN TRỌNG: GỘP FILE & GIẢI NÉN) ---
def clean_text(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""

def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_and_prepare_data():
    # 1. Nếu đã có file DB ngon lành thì dùng luôn
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db()
            res = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'").fetchone()
            conn.close()
            if res and res[0] > 0: return True, "Dữ liệu đã sẵn sàng"
        except:
            os.remove(DB_FILE) # Lỗi thì xóa đi làm lại

    # 2. Kiểm tra các file bị cắt (bhxh_data.zip.001, .002...)
    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Tìm thấy {len(parts)} phần dữ liệu phân mảnh. Đang ghép nối...")
        try:
            # Ghép file
            full_zip = "bhxh_data_full.zip"
            with open(full_zip, 'wb') as outfile:
                for part in parts:
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            
            # Giải nén
            msg.info("📦 Đang giải nén dữ liệu...")
            with zipfile.ZipFile(full_zip, 'r') as zip_ref:
                zip_ref.extractall()
            
            # Dọn dẹp file tạm
            if os.path.exists(full_zip): os.remove(full_zip)
            
            msg.success("✅ Đã khôi phục dữ liệu thành công!")
            time.sleep(1); msg.empty()
            return True, "Restored"
        except Exception as e:
            return False, f"Lỗi ghép file: {str(e)}"

    # 3. Fallback: Nếu chỉ có file Excel gốc (Dùng cho file nhỏ)
    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()

    return False, "⚠️ Không tìm thấy dữ liệu. Hãy upload các file bhxh_data.zip.001, .002... lên GitHub"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp trực tiếp từ Excel (Sẽ chậm). Khuyên dùng tool 'local_converter.py' trên máy tính để tạo file Database phân mảnh rồi up lên.")
    conn = init_data_db()
    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info(f"⏳ Đang xử lý '{EXCEL_FILE}'...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(40)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        for col in df.columns:
            if col != 'master_search_idx': df[f'idx_{col}'] = df[col].apply(clean_text)
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        bar.progress(100); time.sleep(0.5); msg.empty(); bar.empty(); conn.close()
        return True, "Done"
    except Exception as e:
        conn.close(); 
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, str(e)

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor(); c.execute("PRAGMA table_info(bhxh)")
        return [r[1] for r in c.fetchall() if not r[1].startswith('idx_') and r[1] != 'master_search_idx' and r[1] != 'index']
    except: return []
    finally: conn.close()

# --- 3. TÌM KIẾM ---
def search_data(mode, q):
    conn = init_data_db()
    cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        if mode == 'ai':
            key = clean_text(q)
            if not key: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{key}%',))
        elif mode == 'manual':
            conds, vals = [], []
            for c, v in q.items():
                if v and v.strip():
                    db_col = unidecode.unidecode(c).strip().replace(' ', '_').lower()
                    conds.append(f'idx_{db_col} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- 4. GIAO DIỆN ---
def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("User"); p = st.text_input("Pass", type='password')
            if st.form_submit_button("Vào"):
                c = init_user_db()
                res = c.execute('SELECT * FROM users WHERE username=? AND password=?', (u, make_hashes(p))).fetchone()
                c.close()
                if res:
                    st.session_state.update({'logged_in': True, 'username': u, 'role': res[2]})
                    log_action(u, "Login"); st.rerun()
                else: st.error("Sai thông tin")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Dữ Liệu")
    tab1, tab2 = st.tabs(["Thông minh (AI)", "Chính xác (Thủ công)"])
    
    with tab1:
        q = st.text_input("Nhập thông tin bất kỳ:", placeholder="Ví dụ: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df)==1 and configure_gemini():
                    with st.expander("✨ AI Phân tích"):
                        try:
                            model = genai.GenerativeModel('gemini-pro')
                            res = model.generate_content(f"Dữ liệu BHXH: {df.iloc[0].to_dict()}. Tóm tắt quyền lợi.")
                            st.write(res.text)
                        except: st.warning("Lỗi AI")
            else: st.warning("Không tìm thấy")

    with tab2:
        with st.expander("Bộ lọc nâng cao", expanded=True):
            def_cols = [c for c in cols if any(x in c for x in ['ten', 'sinh', 'ma', 'so'])]
            sel = st.multiselect("Chọn cột:", cols, default=def_cols[:3] if def_cols else cols[:3])
            inputs = {c: st.columns(len(sel))[i].text_input(f"Nhập {c}") for i, c in enumerate(sel)} if sel else {}
        
        if st.button("Tìm kiếm"):
            df = search_data('manual', inputs)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
            else: st.warning("Không tìm thấy")

def render_admin():
    st.header("🛠️ Quản Trị")
    conn = init_user_db()
    if st.button("🧹 Xóa Cache & Reset"): st.cache_data.clear(); st.rerun()
    
    t1, t2 = st.tabs(["Người dùng", "Nhật ký"])
    with t1:
        st.dataframe(pd.read_sql("SELECT username, role FROM users", conn), use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            with st.form("add"):
                u = st.text_input("User"); p = st.text_input("Pass", type="password"); r = st.selectbox("Role", ["user", "admin"])
                if st.form_submit_button("Thêm"):
                    try: conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r)); conn.commit(); st.success("Xong"); st.rerun()
                    except: st.error("Trùng tên")
        with c2:
            us = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
            ud = st.selectbox("Xóa User", us) if us else None
            if ud and st.button("Xóa"): conn.execute("DELETE FROM users WHERE username=?", (ud,)); conn.commit(); st.success("Đã xóa"); st.rerun()
            
    with t2:
        if st.button("Xóa Logs"): conn.execute("DELETE FROM logs"); conn.commit(); st.rerun()
        st.dataframe(pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100", conn), use_container_width=True)
    conn.close()

def main():
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'search'})
    
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return

    if not st.session_state['logged_in']: render_login()
    else:
        with st.sidebar:
            st.title(f"Hi, {st.session_state['username']}")
            if st.button("🔍 Tra cứu", use_container_width=True): st.session_state['page'] = 'search'
            if st.session_state['role'] == 'admin':
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            st.divider()
            if st.button("Đăng xuất"): 
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False; st.rerun()

        if st.session_state['page'] == 'search': render_search(get_display_columns())
        elif st.session_state['page'] == 'admin': render_admin()

if __name__ == '__main__': init_user_db(); main()
