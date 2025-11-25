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
# 🔑 KHU VỰC NHẬP API KEY (DÁN KEY CỦA BẠN VÀO DƯỚI ĐÂY)
# ==============================================================================
# Cách lấy key: Vào https://aistudio.google.com/app/apikey -> Create API key
MY_API_KEY = "AIzaSyCw8kpB4mr_rw9IAh3-UOoaQfB8y_x16NE"  # <--- DÁN KEY VÀO GIỮA 2 DẤU NGOẶC KÉP
# ==============================================================================


# Tên file dữ liệu
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'
ZIP_PART_PREFIX = 'bhxh_data.zip.' 
USER_DB = 'users.db'

# --- 1. HỆ THỐNG QUẢN LÝ (USER & LOGS) ---
def init_user_db():
    """Khởi tạo DB User"""
    conn = sqlite3.connect(USER_DB, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    
    # Tự động sửa bảng logs nếu lỗi
    try:
        c.execute("SELECT * FROM logs LIMIT 1")
        cols = [d[0] for d in c.description]
        if "details" not in cols: raise Exception()
    except:
        c.execute("DROP TABLE IF EXISTS logs")
        c.execute('''CREATE TABLE logs 
                     (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    # Tạo admin mặc định
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

# --- 2. HỆ THỐNG AI THÔNG MINH (TỰ DÒ TÌM MODEL) ---
def configure_ai():
    """Cấu hình API Key ưu tiên từ biến hardcoded"""
    # 1. Lấy key từ code (ưu tiên cao nhất)
    api_key = MY_API_KEY
    
    # 2. Nếu trong code để trống, thử lấy từ giao diện
    if not api_key or "AIza" not in api_key:
        api_key = st.session_state.get('user_api_key', '')
    
    # 3. Nếu vẫn không có, thử lấy từ secrets
    if not api_key:
        try: api_key = st.secrets["GOOGLE_API_KEY"]
        except: pass
        
    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

def get_working_model():
    """
    Hàm này tự động hỏi Google xem tài khoản này được dùng model nào.
    Tránh lỗi 404 do gọi sai tên model.
    """
    if 'working_model_name' in st.session_state:
        return st.session_state['working_model_name']

    try:
        # Lấy danh sách model mà key này dùng được
        models = genai.list_models()
        for m in models:
            # Tìm model hỗ trợ tạo nội dung (generateContent)
            if 'generateContent' in m.supported_generation_methods:
                # Ưu tiên các model mới và tốt
                if 'flash' in m.name or 'pro' in m.name:
                    st.session_state['working_model_name'] = m.name
                    return m.name
        
        # Nếu không tìm thấy cái nào ưu tiên, lấy cái đầu tiên tìm được
        for m in models:
            if 'generateContent' in m.supported_generation_methods:
                st.session_state['working_model_name'] = m.name
                return m.name
                
    except Exception as e:
        return None # Lỗi kết nối hoặc sai key
    
    return "gemini-pro" # Fallback cuối cùng

def get_ai_response(prompt, role_desc=""):
    if not configure_ai():
        return "⚠️ Chưa nhập API Key. Vui lòng điền Key vào file code (dòng 22) hoặc nhập trên menu."

    # Tự động chọn model đang sống
    model_name = get_working_model()
    if not model_name:
        return "⚠️ API Key không hợp lệ hoặc lỗi kết nối Google."

    try:
        model = genai.GenerativeModel(model_name)
        full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
        response = model.generate_content(full_prompt)
        return response.text
    except Exception as e:
        # Nếu model tự chọn vẫn lỗi, thử model "huyền thoại" gemini-pro
        try:
            model = genai.GenerativeModel("gemini-pro")
            response = model.generate_content(full_prompt)
            return response.text
        except:
            return f"⚠️ Lỗi AI ({model_name}): {str(e)}"

# --- 3. XỬ LÝ DỮ LIỆU LỚN (SQLITE) ---
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
            if res and res[0] > 0: return True, "Dữ liệu sẵn sàng"
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
            msg.info("📦 Đang giải nén...")
            with zipfile.ZipFile(full_zip, 'r') as zip_ref:
                zip_ref.extractall()
            if os.path.exists(full_zip): os.remove(full_zip)
            msg.success("✅ Xong! Dữ liệu đã sẵn sàng.")
            time.sleep(1); msg.empty()
            return True, "Restored"
        except Exception as e:
            return False, f"Lỗi file: {str(e)}"

    if os.path.exists(EXCEL_FILE): return import_excel_to_sqlite()
    return False, "⚠️ Không tìm thấy dữ liệu (bhxh_data.zip hoặc aaa.xlsb)"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp từ Excel. Khuyên dùng tool 'local_converter.py' để nhanh hơn.")
    conn = init_data_db()
    msg = st.empty(); bar = st.progress(0)
    try:
        msg.info("⏳ Đang đọc file Excel...")
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
        bar.progress(100); msg.empty(); bar.empty(); conn.close()
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
        all = [r[1] for r in c.fetchall()]
        return [c for c in all if not c.startswith('idx_') and c != 'master_search_idx' and 'kcb' not in c.lower() and c != 'index']
    except: return []
    finally: conn.close()

# --- 4. LOGIC TÌM KIẾM ---
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
                    db = unidecode.unidecode(c).strip().replace(' ', '_').lower()
                    conds.append(f'idx_{db} LIKE ?')
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
                    log_action(u, "Login"); st.rerun()
                else: st.error("Sai thông tin")

def render_search(cols):
    st.subheader("🔍 Tra Cứu Dữ Liệu")
    tab1, tab2 = st.tabs(["Nhanh (AI)", "Chi tiết (Thủ công)"])
    
    with tab1:
        st.caption("Nhập tên, số thẻ, ngày sinh (viết liền không dấu)...")
        q = st.text_input("Từ khóa:", placeholder="vd: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df) == 1:
                    with st.expander("✨ AI Phân tích hồ sơ", expanded=True):
                        with st.spinner("AI đang đọc..."):
                            res = get_ai_response(f"Hồ sơ BHXH: {df.iloc[0].to_dict()}. Tóm tắt quyền lợi ngắn gọn.")
                            st.write(res)
            else: st.warning("Không tìm thấy.")

    with tab2:
        default = ['sobhxh', 'hoten', 'ngaysinh', 'socmnd']
        sel_cols = [c for c in cols if any(x in unidecode.unidecode(c).lower() for x in default)]
        if not sel_cols: sel_cols = cols[:4]
        
        with st.expander("⚙️ Cột tìm kiếm", expanded=True):
            selected = st.multiselect("Chọn cột:", cols, default=sel_cols)
        
        inputs = {}
        if selected:
            ui_cols = st.columns(4)
            for i, c in enumerate(selected):
                inputs[c] = ui_cols[i % 4].text_input(f"Nhập {c}")
        
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
    
    # Kiểm tra trạng thái AI
    model_name = get_working_model()
    if not model_name:
        st.error("❌ Lỗi API Key: Vui lòng dán Key vào dòng 22 trong file code app.py")
        return
    
    # Chỉ hiện 1 lần đầu tiên để user yên tâm
    if 'ai_checked' not in st.session_state:
        st.toast(f"✅ Đã kết nối AI thành công! (Model: {model_name})", icon="🤖")
        st.session_state['ai_checked'] = True

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "model", "content": "Chào bạn! Tôi có thể giúp gì về luật BHXH/BHYT?"}]

    for m in st.session_state.messages:
        with st.chat_message(m["role"]): st.markdown(m["content"])

    if prompt := st.chat_input("Nhập câu hỏi..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)

        with st.chat_message("model"):
            with st.spinner("..."):
                role = "Bạn là chuyên gia tư vấn BHXH Việt Nam. Trả lời ngắn gọn, chính xác, thân thiện."
                res = get_ai_response(prompt, role)
                st.markdown(res)
                st.session_state.messages.append({"role": "model", "content": res})

def render_content():
    st.subheader("✍️ Tạo Nội Dung Tuyên Truyền")
    if not get_working_model():
        st.error("Chưa cấu hình API Key.")
        return

    c1, c2 = st.columns(2)
    with c1:
        topic = st.text_input("Chủ đề:", placeholder="Vd: Lợi ích BHYT 5 năm liên tục")
        target = st.selectbox("Đối tượng:", ["Người lao động", "Học sinh", "Người già", "Toàn dân"])
        ctype = st.selectbox("Loại:", ["Bài Facebook (Vui vẻ)", "Thông báo (Trang trọng)", "Kịch bản tư vấn"])
        if st.button("✨ Viết bài ngay", type="primary"):
            if topic:
                with st.spinner("Đang viết..."):
                    res = get_ai_response(f"Viết về: {topic}. Cho: {target}. Dạng: {ctype}. Yêu cầu: Hấp dẫn, có emoji.", "Chuyên viên truyền thông")
                    st.session_state['content'] = res
            else: st.warning("Nhập chủ đề.")
    with c2:
        if 'content' in st.session_state:
            st.text_area("Kết quả:", value=st.session_state['content'], height=400)

def render_admin():
    st.header("🛠️ Quản Trị")
    conn = init_user_db()
    if st.button("🧹 Xóa Cache"): st.cache_data.clear(); st.rerun()

    t1, t2 = st.tabs(["User", "Logs"])
    with t1:
        st.dataframe(pd.read_sql("SELECT username, role FROM users", conn), use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            with st.form("add"):
                u = st.text_input("User"); p = st.text_input("Pass", type="password"); r = st.selectbox("Role", ["user", "admin"])
                if st.form_submit_button("Tạo"):
                    try: conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r)); conn.commit(); st.success("OK"); st.rerun()
                    except: st.error("Trùng tên")
        with c2:
            us = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
            ud = st.selectbox("Xóa", us) if us else None
            if ud and st.button("Xóa"): conn.execute("DELETE FROM users WHERE username=?", (ud,)); conn.commit(); st.success("Xóa xong"); st.rerun()

    with t2:
        if st.button("Xóa Logs"): conn.execute("DELETE FROM logs"); conn.commit(); st.rerun()
        st.dataframe(pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100", conn), use_container_width=True)
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
            st.divider()
            if st.button("🔍 Tra cứu", use_container_width=True): st.session_state['page'] = 'search'
            if st.button("🤖 Chatbot AI", use_container_width=True): st.session_state['page'] = 'chatbot'
            if st.button("✍️ Tạo nội dung", use_container_width=True): st.session_state['page'] = 'content'
            if st.session_state['role'] == 'admin':
                st.divider(); 
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            
            st.divider()
            # Nếu chưa có key cứng thì hiện ô nhập
            if not MY_API_KEY or "AIza" not in MY_API_KEY:
                with st.expander("🔑 API Key"):
                    k = st.text_input("Key", type="password", value=st.session_state.get('user_api_key',''))
                    if k: st.session_state['user_api_key'] = k

            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False; st.rerun()

        cols = get_display_columns()
        p = st.session_state['page']
        if p == 'search': render_search(cols)
        elif p == 'chatbot': render_chatbot()
        elif p == 'content': render_content()
        elif p == 'admin': render_admin()

if __name__ == '__main__':
    init_user_db()
    main()
