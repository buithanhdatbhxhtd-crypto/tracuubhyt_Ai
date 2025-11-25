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
st.set_page_config(
    page_title="Hệ thống BHXH Big Data",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 🔑 API KEY CỦA BẠN (ĐÃ ĐIỀN SẴN)
# ==============================================================================
HARDCODED_API_KEY = "AIzaSyCw8kpB4mr_rw9IAh3-UOoaQfB8y_x16NE"
# ==============================================================================

# Tên file
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB = 'users.db'

# --- 1. HỆ THỐNG QUẢN LÝ ---
def init_user_db():
    """Khởi tạo DB User"""
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
        c.execute("INSERT INTO users VALUES (?, ?, ?)", 
                  ('admin', hashlib.sha256(str.encode('admin123')).hexdigest(), 'admin'))
    conn.commit()
    return conn

def make_hashes(password): return hashlib.sha256(str.encode(password)).hexdigest()
def log_action(u, a, d=""): 
    try: 
        c = init_user_db()
        c.execute("INSERT INTO logs VALUES (?,?,?,?)", (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), u, a, str(d)))
        c.commit(); c.close()
    except: pass

# --- 2. HỆ THỐNG AI THÔNG MINH (AUTO-DETECT MODEL) ---
def configure_ai():
    api_key = HARDCODED_API_KEY
    if not api_key: api_key = st.session_state.get('user_api_key', '')
    if not api_key: api_key = st.secrets.get("GOOGLE_API_KEY", "")
    
    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

def get_working_model_name():
    """
    Hàm này hỏi Google: 'Tôi được dùng những model nào?'
    Sau đó chọn cái tốt nhất có thể dùng.
    """
    if 'valid_model_name' in st.session_state:
        return st.session_state['valid_model_name']
    
    try:
        # Lấy danh sách model khả dụng với Key này
        models = genai.list_models()
        for m in models:
            # Tìm model hỗ trợ chat hoặc text
            if 'generateContent' in m.supported_generation_methods:
                # Ưu tiên Flash hoặc Pro nếu có
                name = m.name.replace('models/', '') # Lấy tên sạch
                if 'flash' in name: 
                    st.session_state['valid_model_name'] = name
                    return name
                
        # Nếu không có flash, lặp lại lấy cái đầu tiên tìm được
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                name = m.name.replace('models/', '')
                st.session_state['valid_model_name'] = name
                return name

    except Exception as e:
        # Nếu lỗi list_models, dùng fallback cứng
        print(f"Lỗi list_models: {e}")
    
    return "gemini-pro" # Model cũ nhưng phổ biến nhất

def get_ai_response(prompt, role_desc=""):
    if not configure_ai(): return "⚠️ Lỗi: Chưa có API Key."

    # Tự động lấy tên model đúng nhất
    model_name = get_working_model_name()
    
    try:
        model = genai.GenerativeModel(model_name)
        full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
        response = model.generate_content(full_prompt)
        return response.text
    except Exception as e:
        # Nếu vẫn lỗi, thử model 'gemini-pro' lần cuối
        try:
            if model_name != 'gemini-pro':
                model = genai.GenerativeModel('gemini-pro')
                response = model.generate_content(full_prompt)
                return response.text
        except:
            pass
        return f"⚠️ AI đang bận ({model_name}). Lỗi: {str(e)}"

# --- 3. XỬ LÝ DỮ LIỆU ---
def clean_text(text): return unidecode.unidecode(str(text)).lower().replace(' ', '') if pd.notna(text) else ""
def init_data_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def check_and_prepare_data():
    if os.path.exists(DB_FILE):
        try:
            conn = init_data_db()
            res = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'").fetchone()
            conn.close()
            if res and res[0] > 0: return True, "Dữ liệu sẵn sàng"
        except: os.remove(DB_FILE)

    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Đang nối {len(parts)} file dữ liệu...")
        try:
            full_zip = "bhxh_data_full.zip"
            with open(full_zip, 'wb') as outfile:
                for part in parts:
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            msg.info("📦 Đang giải nén...")
            with zipfile.ZipFile(full_zip, 'r') as zip_ref:
                zip_ref.extractall()
            if os.path.exists(full_zip): os.remove(full_zip)
            msg.success("✅ Xong!"); time.sleep(0.5); msg.empty(); return True, "Restored"
        except Exception as e: return False, f"Lỗi file: {str(e)}"

    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "⚠️ Thiếu dữ liệu (bhxh_data.zip hoặc aaa.xlsb)"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp Excel (Chậm). Nên dùng tool trên máy tính.")
    conn = init_data_db(); msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang đọc Excel..."); df = pd.read_excel(EXCEL_FILE, engine='pyxlsb'); bar.progress(30)
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
        return [r[1] for r in c.fetchall() if not r[1].startswith('idx_') and r[1] != 'master_search_idx' and 'kcb' not in r[1].lower() and r[1] != 'index']
    except: return []
    finally: conn.close()

# --- 4. LOGIC TÌM KIẾM ---
def search_data(mode, q):
    conn = init_data_db(); cols = get_display_columns()
    if not cols: return pd.DataFrame()
    sel = ", ".join([f'"{c}"' for c in cols])
    try:
        if mode == 'ai':
            key = clean_text(q); 
            if not key: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50', conn, params=(f'%{key}%',))
        elif mode == 'manual':
            conds, vals = [], []
            for c, v in q.items():
                if v and v.strip():
                    conds.append(f'idx_{unidecode.unidecode(c).strip().replace(" ", "_").lower()} LIKE ?')
                    vals.append(f'%{clean_text(v)}%')
            if not conds: return pd.DataFrame()
            return pd.read_sql_query(f'SELECT {sel} FROM bhxh WHERE {" AND ".join(conds)} LIMIT 50', conn, params=tuple(vals))
    except: return pd.DataFrame()
    finally: conn.close()

# --- 5. GIAO DIỆN ---
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
                    st.rerun()
                else: st.error("Sai thông tin")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Dữ Liệu")
    tab1, tab2 = st.tabs(["Nhanh (AI)", "Chi tiết (Thủ công)"])
    with tab1:
        st.caption("Nhập tên, số thẻ, ngày sinh...")
        q = st.text_input("Từ khóa:", placeholder="vd: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df) == 1:
                    with st.expander("✨ AI Phân tích", expanded=True):
                        with st.spinner("AI đang đọc..."):
                            st.write(get_ai_response(f"Dữ liệu: {df.iloc[0].to_dict()}", "Chuyên gia BHXH tóm tắt quyền lợi."))
            else: st.warning("Không tìm thấy.")
    with tab2:
        # Grid layout
        defaults = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        sel_cols = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in defaults)]
        if not sel_cols: sel_cols = cols[:4]
        
        with st.expander("⚙️ Cột tìm kiếm", expanded=True):
            selected = st.multiselect("Chọn cột:", cols, default=sel_cols)
        
        inputs = {}
        if selected:
            cols_per_row = 4
            for i in range(0, len(selected), cols_per_row):
                row_cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    if i + j < len(selected):
                        c_name = selected[i+j]
                        with row_cols[j]:
                            inputs[c_name] = st.text_input(f"Nhập {c_name}")
        
        if st.button("🔍 Tìm kiếm", type="primary"):
            valid = {k: v for k, v in inputs.items() if v.strip()}
            if valid:
                df = search_data('manual', valid)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else: st.warning("Không tìm thấy.")
            else: st.warning("Nhập ít nhất 1 ô.")

def render_chatbot():
    st.subheader("🤖 Trợ lý ảo BHXH")
    if not configure_ai(): st.error("Lỗi API Key"); return
    
    # Debug: Hiển thị model đang dùng để kiểm tra
    if 'ai_debug_show' not in st.session_state:
        model_used = get_working_model_name()
        st.toast(f"Đang dùng AI Model: {model_used}", icon="🤖")
        st.session_state['ai_debug_show'] = True

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "model", "content": "Chào bạn! Tôi là trợ lý ảo BHXH/BHYT."}]

    for m in st.session_state.messages:
        with st.chat_message(m["role"]): st.markdown(m["content"])

    if prompt := st.chat_input("Hỏi gì đó..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)
        with st.chat_message("model"):
            with st.spinner("..."):
                res = get_ai_response(prompt, "Bạn là chuyên gia tư vấn BHXH Việt Nam.")
                st.markdown(res)
                st.session_state.messages.append({"role": "model", "content": res})

def render_content():
    st.subheader("✍️ Tạo Nội Dung")
    if not configure_ai(): st.error("Lỗi API Key"); return
    c1, c2 = st.columns(2)
    with c1:
        topic = st.text_input("Chủ đề:")
        if st.button("Viết bài", type="primary") and topic:
            with st.spinner("Đang viết..."):
                st.session_state['content'] = get_ai_response(f"Viết bài tuyên truyền về: {topic}", "Chuyên viên truyền thông")
    with c2:
        if 'content' in st.session_state: st.text_area("Kết quả:", value=st.session_state['content'], height=400)

def render_admin():
    st.header("🛠️ Quản Trị")
    if st.button("Kiểm tra kết nối AI"):
        st.info(f"API Key đang dùng: {HARDCODED_API_KEY[:5]}...{HARDCODED_API_KEY[-5:]}")
        try:
            models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
            st.success(f"Các model dùng được: {', '.join(models)}")
        except Exception as e:
            st.error(f"Lỗi kết nối Google: {e}")

    conn = init_user_db()
    t1, t2 = st.tabs(["User", "Logs"])
    with t1:
        st.dataframe(pd.read_sql("SELECT username, role FROM users", conn), use_container_width=True)
        with st.form("add"):
            u = st.text_input("User"); p = st.text_input("Pass", type='password'); r = st.selectbox("Quyền", ["user", "admin"])
            if st.form_submit_button("Thêm"):
                try: conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r)); conn.commit(); st.success("OK"); st.rerun()
                except: st.error("Trùng")
        us = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
        if us:
            ud = st.selectbox("Xóa User", us)
            if st.button("Xóa"): conn.execute("DELETE FROM users WHERE username=?", (ud,)); conn.commit(); st.success("Xóa xong"); st.rerun()
    with t2:
        if st.button("Xóa Logs"): conn.execute("DELETE FROM logs"); conn.commit(); st.rerun()
        st.dataframe(pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 500", conn), use_container_width=True)
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
            if st.button("🤖 Chatbot AI", use_container_width=True): st.session_state['page'] = 'chatbot'
            if st.button("✍️ Tạo nội dung", use_container_width=True): st.session_state['page'] = 'content'
            if st.session_state['role'] == 'admin':
                st.divider(); 
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout"); st.session_state['logged_in'] = False; st.rerun()
        
        cols = get_display_columns()
        p = st.session_state['page']
        if p == 'search': render_search(cols)
        elif p == 'chatbot': render_chatbot()
        elif p == 'content': render_content()
        elif p == 'admin': render_admin()

if __name__ == '__main__': init_user_db(); main()
