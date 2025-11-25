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
    key = st.secrets.get("GOOGLE_API_KEY", st.session_state.get('user_api_key', ''))
    if key: 
        genai.configure(api_key=key)
        return True
    return False

# --- HÀM GỌI AI AN TOÀN (FALLBACK MECHANISM) ---
def get_ai_response(prompt, role_desc=""):
    """
    Hàm này sẽ thử các model khác nhau.
    Nếu model mới (1.5) lỗi, nó sẽ tự động dùng model cũ (pro).
    """
    # Danh sách ưu tiên model
    models_to_try = ['gemini-1.5-flash', 'gemini-pro']
    
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            full_prompt = f"{role_desc}\n\n{prompt}" if role_desc else prompt
            response = model.generate_content(full_prompt)
            return response.text
        except Exception as e:
            # Nếu đây là model cuối cùng mà vẫn lỗi thì mới báo lỗi ra ngoài
            if model_name == models_to_try[-1]:
                return f"⚠️ Hệ thống AI đang bận hoặc gặp lỗi: {str(e)}"
            # Nếu chưa phải cuối cùng, thử model tiếp theo (Silent retry)
            continue
            
    return "Không thể kết nối tới AI."

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

    return False, "⚠️ Không tìm thấy dữ liệu. Hãy upload các file bhxh_data.zip.001... lên GitHub"

def import_excel_to_sqlite():
    st.warning("⚠️ Đang nạp trực tiếp từ Excel. Khuyên dùng tool 'local_converter.py' để tối ưu.")
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
    tab1, tab2 = st.tabs(["Tra cứu Nhanh (AI)", "Tra cứu Chính xác (Thủ công)"])
    
    with tab1:
        st.info("Nhập thông tin bất kỳ: Tên, số thẻ, ngày sinh...")
        q = st.text_input("Từ khóa:", placeholder="Ví dụ: nguyen van a 1990")
        if q:
            df = search_data('ai', q)
            if not df.empty:
                st.success(f"Tìm thấy {len(df)} kết quả")
                st.dataframe(df, use_container_width=True, hide_index=True)
                if len(df)==1 and configure_gemini():
                    with st.expander("✨ AI Phân tích hồ sơ"):
                        with st.spinner("AI đang đọc dữ liệu..."):
                            # Dùng hàm get_ai_response thay vì gọi trực tiếp
                            role = "Bạn là chuyên gia BHXH. Hãy tóm tắt quyền lợi bảo hiểm cho người này dựa trên dữ liệu."
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
    
    if not configure_gemini():
        st.warning("Vui lòng nhập API Key ở thanh bên trái để sử dụng tính năng này.")
        return

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "model", "content": "Chào bạn! Tôi là trợ lý ảo chuyên về BHXH, BHYT. Bạn cần tôi giúp gì hôm nay?"}
        ]

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Nhập câu hỏi của bạn..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("model"):
            with st.spinner("Đang suy nghĩ..."):
                role_desc = """
                Bạn là một chuyên gia tư vấn về Bảo hiểm xã hội (BHXH) và Bảo hiểm y tế (BHYT) tại Việt Nam. 
                Nhiệm vụ của bạn là trả lời các câu hỏi của người dân một cách chính xác, dễ hiểu, trích dẫn luật nếu cần.
                Hãy giữ thái độ thân thiện, chuyên nghiệp.
                """
                # Dùng hàm get_ai_response an toàn
                response_text = get_ai_response(prompt, role_desc)
                st.markdown(response_text)
                st.session_state.messages.append({"role": "model", "content": response_text})

def render_content_creator():
    st.subheader("✍️ Sáng Tạo Nội Dung Tuyên Truyền")
    st.caption("Công cụ hỗ trợ viết bài đăng Facebook, Zalo, Thông báo cổ động.")

    if not configure_gemini():
        st.warning("Vui lòng nhập API Key để sử dụng.")
        return

    col1, col2 = st.columns([1, 1])
    
    with col1:
        topic = st.text_input("Chủ đề bài viết:", placeholder="Ví dụ: Lợi ích của BHXH tự nguyện")
        target_audience = st.selectbox("Đối tượng:", ["Người lao động tự do", "Học sinh sinh viên", "Người cao tuổi", "Doanh nghiệp", "Toàn dân"])
        content_type = st.selectbox("Loại nội dung:", ["Bài đăng Facebook (Vui vẻ)", "Thông báo hành chính", "Khẩu hiệu cổ động", "Kịch bản tư vấn"])
        
        if st.button("✨ Tạo nội dung", type="primary"):
            if topic:
                with st.spinner("AI đang viết bài..."):
                    role = "Bạn là chuyên viên truyền thông BHXH."
                    prompt = f"Viết nội dung về: {topic}. Đối tượng: {target_audience}. Dạng: {content_type}. Yêu cầu: Hấp dẫn, chuẩn mực, có emoji và hashtag."
                    
                    # Dùng hàm get_ai_response an toàn
                    res = get_ai_response(prompt, role)
                    st.session_state['generated_content'] = res
            else:
                st.warning("Vui lòng nhập chủ đề.")

    with col2:
        st.write("### Kết quả:")
        if 'generated_content' in st.session_state:
            st.text_area("Nội dung đã tạo (Copy để dùng):", value=st.session_state['generated_content'], height=400)
        else:
            st.info("Kết quả sẽ hiển thị tại đây sau khi bạn bấm nút Tạo.")

def render_admin():
    st.header("🛠️ Quản Trị Hệ Thống")
    conn = init_user_db()
    
    if st.button("🧹 Xóa Cache & Reset Ứng Dụng"):
        st.cache_data.clear()
        st.rerun()

    t1, t2 = st.tabs(["👥 Quản lý Người dùng", "📜 Nhật ký Hoạt động"])
    
    with t1:
        st.subheader("Danh sách tài khoản")
        users_df = pd.read_sql("SELECT username, role FROM users", conn)
        st.dataframe(users_df, use_container_width=True, hide_index=True)
        st.divider()
        
        c_add, c_del = st.columns(2)
        with c_add:
            st.write("##### ➕ Thêm User Mới")
            with st.form("add_user_form", clear_on_submit=True):
                new_u = st.text_input("Username")
                new_p = st.text_input("Password", type="password")
                new_r = st.selectbox("Quyền hạn", ["user", "admin"])
                if st.form_submit_button("Tạo tài khoản"):
                    if new_u and new_p:
                        try:
                            conn.execute("INSERT INTO users VALUES (?,?,?)", (new_u, make_hashes(new_p), new_r))
                            conn.commit()
                            st.success(f"Đã tạo user: {new_u}")
                            log_action(st.session_state['username'], "Add User", new_u)
                            time.sleep(1); st.rerun()
                        except: st.error("Tên đã tồn tại!")
                    else: st.warning("Nhập thiếu thông tin.")

        with c_del:
            st.write("##### 🗑️ Xóa User")
            user_list = [u for u in users_df['username'] if u != 'admin']
            if user_list:
                user_to_del = st.selectbox("Chọn user cần xóa:", user_list)
                if st.button("Xóa vĩnh viễn", type="primary"):
                    conn.execute("DELETE FROM users WHERE username=?", (user_to_del,))
                    conn.commit()
                    st.success(f"Đã xóa user: {user_to_del}")
                    log_action(st.session_state['username'], "Delete User", user_to_del)
                    time.sleep(1); st.rerun()
            else: st.info("Không có tài khoản phụ.")

    with t2:
        st.subheader("Lịch sử truy cập")
        if st.button("Xóa toàn bộ Nhật ký"):
            conn.execute("DELETE FROM logs")
            conn.commit(); st.success("Đã dọn dẹp!"); st.rerun()
        logs_df = pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 500", conn)
        st.dataframe(logs_df, use_container_width=True, hide_index=True)
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
            st.title(f"Xin chào, {st.session_state['username']}")
            
            st.markdown("---")
            if st.button("🔍 Tra cứu Dữ liệu", use_container_width=True): st.session_state['page'] = 'search'
            if st.button("🤖 Chatbot Tư vấn", use_container_width=True): st.session_state['page'] = 'chatbot'
            if st.button("✍️ Tạo bài Tuyên truyền", use_container_width=True): st.session_state['page'] = 'content_creator'
            
            if st.session_state['role'] == 'admin':
                st.markdown("---")
                if st.button("🛠️ Quản trị hệ thống", use_container_width=True): st.session_state['page'] = 'admin'
            
            st.markdown("---")
            with st.expander("🔑 Cấu hình AI Key"):
                k = st.text_input("Google API Key", type="password", value=st.session_state.get('user_api_key',''))
                if k: st.session_state['user_api_key'] = k

            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        cols = get_display_columns()
        page = st.session_state['page']
        
        if page == 'search': render_search(cols)
        elif page == 'chatbot': render_chatbot()
        elif page == 'content_creator': render_content_creator()
        elif page == 'admin': render_admin()

if __name__ == '__main__':
    init_user_db()
    main()
