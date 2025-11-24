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

# Tên file dữ liệu gốc và file database
EXCEL_FILE = 'aaa.xlsb'
DB_FILE = 'bhxh_data.db'

# --- 1. CÁC HÀM XỬ LÝ USER & LOG (GIỮ NGUYÊN) ---
def init_user_db():
    """Khởi tạo DB cho user và logs (tách biệt với data nghiệp vụ)"""
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs
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
    try:
        conn = init_user_db()
        c = conn.cursor()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, details))
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

# --- 2. XỬ LÝ DỮ LIỆU LỚN (CHUYỂN ĐỔI EXCEL -> SQLITE) ---

@st.cache_resource
def init_data_connection():
    """Kết nối tới database dữ liệu nghiệp vụ"""
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def clean_text(text):
    if pd.isna(text) or text == "": return ""
    return unidecode.unidecode(str(text)).lower().replace(' ', '')

def import_excel_to_sqlite():
    """
    Hàm này chỉ chạy 1 lần đầu tiên để chuyển Excel 500k dòng sang SQLite.
    """
    if not os.path.exists(EXCEL_FILE):
        return False, f"Không tìm thấy file '{EXCEL_FILE}'"

    try:
        # Kiểm tra xem đã import chưa bằng cách check bảng
        conn = init_data_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bhxh'")
        if cursor.fetchone():
            conn.close()
            return True, "Dữ liệu đã sẵn sàng." # Đã có dữ liệu, không cần import lại

        # Bắt đầu Import (Tiến trình này có thể mất 1-2 phút với 500k dòng)
        status_placeholder = st.empty()
        status_placeholder.info("🚀 Đang khởi tạo hệ thống dữ liệu lớn (Lần đầu tiên)... Vui lòng đợi.")
        
        # Đọc file Excel theo chunk (nếu file quá lớn, nhưng pyxlsb thường đọc 1 lèo cũng ổn với 500k)
        # Ở đây đọc hết 1 lần cho nhanh nếu RAM server > 1GB, nếu yếu hơn thì cần chia nhỏ.
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        
        # Chuẩn hóa tên cột (bỏ dấu cách, ký tự lạ để làm tên cột SQL)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').lower() for c in df.columns]
        
        # Chuyển tất cả về string
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT'], '', inplace=True)
        
        # TẠO CỘT SEARCH INDEX (QUAN TRỌNG NHẤT)
        # Gộp nội dung lại để tìm kiếm full-text
        status_placeholder.info("⚙️ Đang tối ưu hóa chỉ mục tìm kiếm...")
        
        # Tạo cột tìm kiếm tổng hợp (Master Search)
        df['master_search_idx'] = df.apply(lambda x: clean_text(' '.join(x.values)), axis=1)
        
        # Tạo các cột index riêng lẻ cho từng trường quan trọng (để tìm thủ công nhanh)
        # Ví dụ: hoten -> idx_hoten
        for col in df.columns:
            if col != 'master_search_idx':
                df[f'idx_{col}'] = df[col].apply(clean_text)

        # Ghi vào SQLite
        status_placeholder.info("💾 Đang lưu trữ vào cơ sở dữ liệu...")
        df.to_sql('bhxh', conn, if_exists='replace', index=False)
        
        # Tạo Index cho cột master_search_idx để tìm siêu nhanh
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        conn.commit()
        conn.close()
        
        status_placeholder.success("✅ Hoàn tất nạp dữ liệu!")
        time.sleep(2)
        status_placeholder.empty()
        return True, "Đã nạp dữ liệu mới thành công."
        
    except Exception as e:
        return False, f"Lỗi nạp dữ liệu: {str(e)}"

def get_table_columns():
    conn = init_data_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(bhxh)")
    columns = [info[1] for info in cursor.fetchall()]
    conn.close()
    # Lọc bỏ các cột idx_ và master_search_idx để lấy cột gốc hiển thị
    real_cols = [c for c in columns if not c.startswith('idx_') and c != 'master_search_idx']
    return real_cols

# --- 3. LOGIC TRA CỨU SQL (SIÊU NHANH) ---

def sql_search(query_type, params):
    """
    Hàm tìm kiếm dùng SQL Query.
    query_type: 'ai' hoặc 'manual'
    params: từ khóa hoặc dict các bộ lọc
    """
    conn = init_data_connection()
    
    # Lấy danh sách cột hiển thị
    real_cols = get_table_columns()
    select_cols = ", ".join(real_cols)
    
    try:
        if query_type == 'ai':
            keyword = clean_text(params)
            # Dùng LIKE '%keyword%' trên cột index
            sql = f"SELECT {select_cols} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 100"
            df = pd.read_sql_query(sql, conn, params=(f'%{keyword}%',))
            
        elif query_type == 'manual':
            # Xây dựng câu query động: WHERE idx_col1 LIKE %v1% AND idx_col2 LIKE %v2%
            conditions = []
            values = []
            for col, val in params.items():
                if val:
                    conditions.append(f"idx_{col} LIKE ?")
                    values.append(f"%{clean_text(val)}%")
            
            where_clause = " AND ".join(conditions)
            sql = f"SELECT {select_cols} FROM bhxh WHERE {where_clause} LIMIT 100"
            df = pd.read_sql_query(sql, conn, params=tuple(values))
            
        conn.close()
        return df
    except Exception as e:
        conn.close()
        st.error(f"Lỗi truy vấn: {e}")
        return pd.DataFrame()

# --- 4. GIAO DIỆN ---

def render_login():
    st.markdown("<h2 style='text-align: center;'>🔐 Hệ Thống Tra Cứu Dữ Liệu Lớn</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập")
            password = st.text_input("Mật khẩu", type='password')
            submitted = st.form_submit_button("Đăng nhập", use_container_width=True)
            
            if submitted:
                conn = init_user_db()
                c = conn.cursor()
                c.execute('SELECT * FROM users WHERE username = ? AND password = ?', 
                          (username, make_hashes(password)))
                data = c.fetchall()
                conn.close()
                if data:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.session_state['role'] = data[0][2]
                    log_action(username, "Login", "Success")
                    st.rerun()
                else:
                    st.error("Sai thông tin đăng nhập.")

def render_manual_search(cols_list):
    st.subheader("📋 Tra Cứu Thủ Công (Chính Xác)")
    st.caption("Dữ liệu 500.000+ bản ghi. Nhập thông tin không dấu viết liền cũng được.")
    
    with st.expander("⚙️ Chọn trường tìm kiếm", expanded=True):
        default = []
        # Gợi ý cột quan trọng (dựa trên tên cột chuẩn hóa lowercase)
        for c in ['hoten', 'ngaysinh', 'socmnd', 'cccd', 'mabhxh', 'mathe']:
            for col in cols_list:
                if c in col: default.append(col)
        if not default: default = cols_list[:3]
        
        selected_filters = st.multiselect("Tiêu chí:", cols_list, default=default)

    inputs = {}
    if selected_filters:
        cols = st.columns(len(selected_filters))
        for i, col in enumerate(selected_filters):
            inputs[col] = cols[i].text_input(f"Nhập {col}")

    if st.button("🔍 Tìm kiếm", type="primary"):
        valid_filters = {k: v for k, v in inputs.items() if v.strip()}
        if valid_filters:
            df_res = sql_search('manual', valid_filters)
            st.success(f"Tìm thấy {len(df_res)} kết quả (Hiển thị tối đa 100).")
            st.dataframe(df_res, use_container_width=True)
            log_action(st.session_state['username'], "Manual Search", str(valid_filters))
        else:
            st.warning("Vui lòng nhập ít nhất 1 trường.")

def render_ai_search(is_ai_ready):
    st.subheader("🤖 Tra Cứu Siêu Tốc & AI")
    st.caption("Tìm kiếm trên toàn bộ cơ sở dữ liệu.")
    
    query = st.text_input("Nhập bất kỳ thông tin nào (Tên, ngày sinh, thẻ...):", placeholder="Ví dụ: nguyenvana 1990")
    
    if query:
        df_res = sql_search('ai', query)
        count = len(df_res)
        
        if count > 0:
            st.success(f"Tìm thấy {count} hồ sơ khớp.")
            st.dataframe(df_res, use_container_width=True)
            
            if count <= 3:
                st.markdown("---")
                if is_ai_ready:
                    for idx, row in df_res.iterrows():
                        with st.expander(f"Phân tích AI: {row.iloc[0]}", expanded=True):
                            with st.spinner("AI đang đọc..."):
                                try:
                                    model = genai.GenerativeModel('gemini-pro')
                                    prompt = f"Dữ liệu BHXH: {row.to_dict()}. Tóm tắt thông tin và quyền lợi người này bằng tiếng Việt."
                                    response = model.generate_content(prompt)
                                    st.write(response.text)
                                except Exception as e: st.error(str(e))
                else:
                    st.warning("Cần nhập API Key để dùng AI.")
            log_action(st.session_state['username'], "AI Search", query)
        else:
            st.warning("Không tìm thấy kết quả.")

def render_admin():
    st.header("Quản Trị Hệ Thống")
    conn = init_user_db()
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Thêm User")
        with st.form("add"):
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            r = st.selectbox("Role", ["user", "admin"])
            if st.form_submit_button("Tạo"):
                try:
                    conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r))
                    conn.commit()
                    st.success(f"Đã tạo {u}")
                    time.sleep(1)
                    st.rerun()
                except: st.error("Tên tồn tại")
    
    with c2:
        st.subheader("Xóa User")
        users = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
        if users:
            u_del = st.selectbox("Chọn user", users)
            if st.button("Xóa User"):
                conn.execute("DELETE FROM users WHERE username=?", (u_del,))
                conn.commit()
                st.success("Đã xóa")
                time.sleep(1)
                st.rerun()
        else: st.info("Chưa có user phụ.")
    
    st.divider()
    st.subheader("Nhật ký")
    if st.button("Xóa Logs"):
        conn.execute("DELETE FROM logs")
        conn.commit()
        st.rerun()
    
    logs = pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100", conn)
    st.dataframe(logs, use_container_width=True)
    conn.close()

# --- MAIN ---
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    # Bước 1: Đảm bảo Data sẵn sàng
    success, msg = import_excel_to_sqlite()
    if not success:
        st.error(f"Lỗi khởi tạo dữ liệu: {msg}")
        return

    if not st.session_state['logged_in']:
        render_login()
    else:
        with st.sidebar:
            st.title(f"Chào {st.session_state['username']}")
            with st.popover("API Key AI"):
                k = st.text_input("Google API Key", type="password", value=st.session_state.get('user_api_key',''))
                if k: st.session_state['user_api_key'] = k
            
            st.divider()
            if 'page' not in st.session_state: st.session_state['page'] = 'search_ai'
            
            if st.button("🤖 Tra cứu AI", use_container_width=True): st.session_state['page'] = 'search_ai'
            if st.button("📋 Tra cứu Thủ công", use_container_width=True): st.session_state['page'] = 'search_manual'
            if st.session_state['role'] == 'admin':
                if st.button("🛠️ Quản trị", use_container_width=True): st.session_state['page'] = 'admin'
            
            st.divider()
            if st.button("Đăng xuất"):
                st.session_state['logged_in'] = False
                st.rerun()

        cols = get_table_columns()
        is_ai = configure_gemini()
        
        if st.session_state['page'] == 'search_ai':
            render_ai_search(is_ai)
        elif st.session_state['page'] == 'search_manual':
            render_manual_search(cols)
        elif st.session_state['page'] == 'admin':
            render_admin()
        
        # Force rerun để cập nhật giao diện nếu vừa chuyển trang
        if 'rerun_trigger' in st.session_state:
            del st.session_state['rerun_trigger']
            st.rerun()

if __name__ == '__main__':
    main()
