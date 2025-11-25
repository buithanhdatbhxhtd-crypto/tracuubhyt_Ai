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
    page_title="Hệ thống BHXH Thông Minh",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 🔑 CẤU HÌNH API KEY (ĐÃ ĐIỀN SẴN) ---
HARDCODED_API_KEY = "AIzaSyCw8kpB4mr_rw9IAh3-UOoaQfB8y_x16NE" 

# Tên file
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB = 'users.db'

# --- 1. HỆ THỐNG USER & LOG ---
def init_user_db():
    """Khởi tạo DB User + Kích hoạt WAL Mode"""
    conn = sqlite3.connect(USER_DB, check_same_thread=False)
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
    # 1. Ưu tiên key cứng trong code
    if HARDCODED_API_KEY:
        genai.configure(api_key=HARDCODED_API_KEY)
        return True

    # 2. Nếu không có, lấy từ giao diện
    key = st.secrets.get("GOOGLE_API_KEY", st.session_state.get('user_api_key', ''))
    if key: 
        genai.configure(api_key=key)
        return True
    return False

# --- HÀM GỌI AI MỚI NHẤT (FIX LỖI 404) ---
def get_ai_response(prompt, role_desc=""):
    if not configure_gemini():
        return "⚠️ Lỗi: Chưa có API Key."

    # Chỉ dùng model mới nhất, ổn định nhất hiện nay
    model_name = 'gemini-1.5-flash'
    
    try:
        model = genai.GenerativeModel(model_name)
        full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
        response = model.generate_content(full_prompt)
        return response.text
    except Exception as e:
        return f"⚠️ Lỗi kết nối AI ({model_name}): {str(e)}\n\n👉 Gợi ý: Hãy thử Refresh lại trang web."

# --- 2. XỬ LÝ DỮ LIỆU ---
def clean_text(text):
    if pd.isna(text) or str(text).lower() == 'nan' or str(text).strip() == '': return ""
    return unidecode.unidecode(str(text)).lower().replace(' ', '')

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
            if res and res[0] > 0: return True, "Dữ liệu đã sẵn sàng"
        except:
            os.remove(DB_FILE) 

    parts = sorted(glob.glob(f"{ZIP_PART_PREFIX}*"))
    if parts:
        msg = st.empty()
        msg.info(f"📦 Tìm thấy {len(parts)} phần dữ liệu. Đang ghép nối...")
        try:
            full_zip = "bhxh_data_full.zip"
            with open(full_zip, 'wb') as outfile:
                for part in parts:
                    with open(part, 'rb') as infile:
                        outfile.write(infile.read())
            
            msg.info("📦 Đang giải nén dữ liệu...")
            with zipfile.ZipFile(full_zip, 'r') as zip_ref:
                zip_ref.extractall()
            
            if os.path.exists(full_zip): os.remove(full_zip)
            
            msg.success("✅ Đã khôi phục dữ liệu thành công!")
            time.sleep(1)
            msg.empty()
            return True, "Restored"
        except Exception as e:
            return False, f"Lỗi ghép file: {str(e)}"

    if os.path.exists(EXCEL_FILE):
        return import_excel_to_sqlite()

    return False, "⚠️ Không tìm thấy dữ liệu. Hãy upload file bhxh_data.zip... lên GitHub"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp từ Excel. Khuyên dùng tool 'local_converter.py' để nhanh hơn.")
    conn = init_data_db()
    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info(f"⏳ Đang xử lý '{EXCEL_FILE}'...")
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(30)
        
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        
        for col in df.columns:
            if col != 'master_search_idx':
                df[f'idx_{col}'] = df[col].apply(clean_text)
        
        bar.progress(80)
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=5000)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        
        bar.progress(100); time.sleep(0.5); msg.empty(); bar.empty(); conn.close()
        return True, "Done"
    except Exception as e:
        conn.close()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        return False, str(e)

@st.cache_data(ttl=3600)
def get_display_columns():
    conn = init_data_db()
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(bhxh)")
        all_cols = [r[1] for r in c.fetchall()]
        clean_cols = []
        for col in all_cols:
            if col.startswith('idx_') or col == 'master_search_idx' or col == 'index': continue
            if 'kcb' in col.lower(): continue
            clean_cols.append(col)
        return clean_cols
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
                    log_action(u, "Login", "Thành công")
                    st.rerun()
                else: st.error("Sai thông tin")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Dữ Liệu")
    tab1, tab2 = st.tabs(["Tra cứu Nhanh", "Tra cứu Chính xác"])
    
    with tab1:
        st.info("Nhập thông tin bất kỳ: Tên, số thẻ, ngày sinh...")
        q = st.text_input("Từ khóa:", placeholder="Ví dụ: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df)==1:
                    with st.expander("✨ AI Phân tích hồ sơ", expanded=True):
                        with st.spinner("AI đang đọc dữ liệu..."):
                            role = "Bạn là chuyên gia BHXH. Hãy tóm tắt quyền lợi từ dữ liệu này. Trả lời ngắn gọn."
                            res = get_ai_response(f"Dữ liệu: {df.iloc[0].to_dict()}", role)
                            st.write(res)
            else: st.warning("Không tìm thấy kết quả.")

    with tab2:
        priority_cols = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        default_selection = []
        for p_col in priority_cols:
            for col in cols:
                if p_col in unidecode.unidecode(col).lower():
                    default_selection.append(col)
                    break
        if not default_selection: default_selection = cols[:4]

        with st.expander("⚙️ Cấu hình cột tìm kiếm", expanded=True):
            selected_cols = st.multiselect("Chọn cột nhập liệu:", cols, default=default_selection)
        
        inputs = {}
        if selected_cols:
            cols_per_row = 4
            for i in range(0, len(selected_cols), cols_per_row):
                row_cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    if i + j < len(selected_cols):
                        col_name = selected_cols[i+j]
                        with row_cols[j]:
                            inputs[col_name] = st.text_input(f"Nhập {col_name}", key=f"search_{col_name}")
        
        if st.button("🔍 Tìm kiếm ngay", type="primary"):
            valid_inputs = {k: v for k, v in inputs.items() if v.strip()}
            if valid_inputs:
                df = search_data('manual', valid_inputs)
                if not df.empty:
                    st.success(f"Tìm thấy {len(df)} kết quả")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    log_action(st.session_state['username'], "Search Manual", str(valid_inputs))
                else: st.warning("Không tìm thấy dữ liệu phù hợp.")
            else: st.warning("Vui lòng nhập ít nhất một thông tin.")

def render_chatbot():
    st.subheader("🤖 Trợ lý ảo BHXH/BHYT")
    
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "model", "content": "Chào bạn! Tôi có thể giúp gì về BHXH/BHYT?"}]

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Nhập câu hỏi..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)

        with st.chat_message("model"):
            with st.spinner("..."):
                role = "Bạn là chuyên gia tư vấn BHXH/BHYT Việt Nam. Trả lời thân thiện, chính xác."
                res = get_ai_response(prompt, role)
                st.markdown(res)
                st.session_state.messages.append({"role": "model", "content": res})

def render_content_creator():
    st.subheader("✍️ Sáng Tạo Nội Dung")
    
    c1, c2 = st.columns(2)
    with c1:
        topic = st.text_input("Chủ đề:", placeholder="Vd: Lợi ích BHXH tự nguyện")
        target = st.selectbox("Đối tượng:", ["Người lao động", "Sinh viên", "Người cao tuổi", "Toàn dân"])
        ctype = st.selectbox("Loại:", ["Bài đăng Facebook", "Thông báo", "Khẩu hiệu", "Kịch bản"])
        
        if st.button("✨ Tạo nội dung", type="primary"):
            if topic:
                with st.spinner("Đang viết..."):
                    role = "Bạn là chuyên viên truyền thông BHXH."
                    prompt = f"Viết về: {topic}. Cho: {target}. Dạng: {ctype}. Yêu cầu: Hấp dẫn, có emoji."
                    res = get_ai_response(prompt, role)
                    st.session_state['content'] = res
            else: st.warning("Nhập chủ đề.")

    with c2:
        st.write("### Kết quả:")
        if 'content' in st.session_state:
            st.text_area("Nội dung:", value=st.session_state['content'], height=400)

def render_admin():
    st.header("🛠️ Quản Trị")
    conn = init_user_db()
    if st.button("🧹 Xóa Cache"): st.cache_data.clear(); st.rerun()

    t1, t2 = st.tabs(["Người dùng", "Nhật ký"])
    with t1:
        st.dataframe(pd.read_sql("SELECT username, role FROM users", conn), use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            with st.form("add"):
                u = st.text_input("User"); p = st.text_input("Pass", type="password"); r = st.selectbox("Quyền", ["user", "admin"])
                if st.form_submit_button("Tạo"):
                    try: conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r)); conn.commit(); st.success("OK"); st.rerun()
                    except: st.error("Trùng tên")
        with c2:
            us = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
            ud = st.selectbox("Xóa User", us) if us else None
            if ud and st.button("Xóa"): conn.execute("DELETE FROM users WHERE username=?", (ud,)); conn.commit(); st.success("Xóa xong"); st.rerun()

    with t2:
        if st.button("Xóa Nhật ký"): conn.execute("DELETE FROM logs"); conn.commit(); st.rerun()
        st.dataframe(pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 500", conn), use_container_width=True)
    conn.close()

# --- MAIN ---
def main():
    if 'logged_in' not in st.session_state: st.session_state.update({'logged_in': False, 'page': 'search'})
    
    ok, msg = check_and_prepare_data()
    if not ok: st.error(msg); return

    if not st.session_state['logged_in']:
        render_login()
    else:
        with st.sidebar:
            st.title(f"Hi, {st.session_state['username']}")
            st.markdown("---")
            if st.button("🔍 Tra cứu", use_container_width=True): st.session_state['page'] = 'search'
            if st.button("🤖 Chatbot", use_container_width=True): st.session_state['page'] = 'chatbot'
            if st.button("✍️ Tạo nội dung", use_container_width=True): st.session_state['page'] = 'content'
            
            if st.session_state['role'] == 'admin':
                st.markdown("---")
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            
            st.markdown("---")
            # Ẩn ô nhập key vì đã có key cứng
            if not HARDCODED_API_KEY:
                with st.expander("🔑 API Key"):
                    k = st.text_input("Key", type="password", value=st.session_state.get('user_api_key',''))
                    if k: st.session_state['user_api_key'] = k

            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        cols = get_display_columns()
        p = st.session_state['page']
        if p == 'search': render_search(cols)
        elif p == 'chatbot': render_chatbot()
        elif p == 'content': render_content_creator()
        elif p == 'admin': render_admin()

if __name__ == '__main__':
    init_user_db()
    main()
