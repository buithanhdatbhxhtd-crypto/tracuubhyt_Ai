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

# --- 1. CÁC HÀM XỬ LÝ USER & LOG ---
def init_user_db():
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

# --- 2. XỬ LÝ DỮ LIỆU LỚN (QUAN TRỌNG NHẤT) ---

def clean_text(text):
    if pd.isna(text) or str(text).lower() == 'nan' or str(text).strip() == '': 
        return ""
    # Chuyển thành chuỗi, xóa dấu, xóa khoảng trắng thừa, viết thường
    text_str = str(text).strip()
    return unidecode.unidecode(text_str).lower().replace(' ', '')

def init_data_db():
    """Kết nối an toàn đến DB dữ liệu"""
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def import_excel_to_sqlite():
    """
    Quy trình nạp dữ liệu an toàn:
    1. Kiểm tra DB có bảng 'bhxh' chưa.
    2. Nếu chưa, đọc Excel, xử lý và lưu vào DB.
    3. Tạo chỉ mục (Index) để tìm kiếm nhanh.
    """
    if not os.path.exists(EXCEL_FILE):
        return False, f"⚠️ Không tìm thấy file '{EXCEL_FILE}'. Hãy upload file vào cùng thư mục."

    conn = init_data_db()
    cursor = conn.cursor()
    
    # Kiểm tra xem bảng đã tồn tại và có dữ liệu chưa
    try:
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bhxh'")
        table_exists = cursor.fetchone()[0]
        if table_exists:
            # Kiểm tra xem bảng có dữ liệu không
            cursor.execute("SELECT count(*) FROM bhxh")
            count = cursor.fetchone()[0]
            if count > 0:
                conn.close()
                return True, f"Dữ liệu đã sẵn sàng ({count} bản ghi)."
            else:
                # Bảng rỗng, xóa đi làm lại
                cursor.execute("DROP TABLE bhxh")
    except:
        pass # Lỗi gì đó thì cứ tiếp tục nạp lại

    # BẮT ĐẦU QUÁ TRÌNH NẠP (Hiển thị Progress Bar)
    msg_placeholder = st.empty()
    bar = st.progress(0)
    
    try:
        msg_placeholder.info(f"⏳ Đang đọc file '{EXCEL_FILE}'... (Việc này mất khoảng 1-2 phút lần đầu)")
        
        # Đọc file Excel
        df = pd.read_excel(EXCEL_FILE, engine='pyxlsb')
        bar.progress(30)
        
        msg_placeholder.info("⚙️ Đang xử lý và làm sạch dữ liệu...")
        
        # 1. Chuẩn hóa tên cột (Xóa khoảng trắng, ký tự lạ để tránh lỗi SQL)
        df.columns = [unidecode.unidecode(str(c)).strip().replace(' ', '_').replace('.','').lower() for c in df.columns]
        
        # 2. Chuyển đổi dữ liệu sang String để tránh lỗi
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT', '<NA>'], '', inplace=True)
        bar.progress(50)

        # 3. TẠO CỘT TÌM KIẾM (INDEX)
        # Thay vì xử lý từng dòng (chậm), ta dùng vector hóa của Pandas (nhanh gấp 100 lần)
        
        # Cột Master Index: Gộp tất cả các trường lại
        df['master_search_idx'] = df.apply(lambda x: ' '.join(x.values), axis=1)
        # Xóa dấu và khoảng trắng cho cột Master
        df['master_search_idx'] = df['master_search_idx'].apply(clean_text)
        
        # Cột Index riêng cho từng trường (để tìm chính xác)
        for col in df.columns:
            if col != 'master_search_idx' and not col.startswith('idx_'):
                df[f'idx_{col}'] = df[col].apply(clean_text)
        
        bar.progress(70)
        msg_placeholder.info("💾 Đang lưu vào Database (Bước này quan trọng nhất)...")
        
        # Lưu vào SQLite
        # chunksize giúp chia nhỏ dữ liệu khi ghi, tránh treo máy
        df.to_sql('bhxh', conn, if_exists='replace', index=False, chunksize=10000)
        
        bar.progress(90)
        msg_placeholder.info("🚀 Đang tạo chỉ mục tốc độ cao...")
        
        # Tạo Index SQL để tìm kiếm tức thì
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_master ON bhxh (master_search_idx)")
        conn.commit()
        
        bar.progress(100)
        time.sleep(1)
        msg_placeholder.empty()
        bar.empty()
        
        conn.close()
        return True, "Nạp dữ liệu thành công!"

    except Exception as e:
        conn.close()
        # Nếu lỗi, xóa file DB hỏng để lần sau chạy lại từ đầu
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        return False, f"Lỗi nạp dữ liệu: {str(e)}"

def get_display_columns():
    """Lấy danh sách cột gốc để hiển thị (bỏ qua các cột index hệ thống)"""
    conn = init_data_db()
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(bhxh)")
        columns = [info[1] for info in cursor.fetchall()]
        # Lọc bỏ các cột bắt đầu bằng idx_ hoặc là master_search_idx
        real_cols = [c for c in columns if not c.startswith('idx_') and c != 'master_search_idx' and c != 'index']
        return real_cols
    except:
        return []
    finally:
        conn.close()

# --- 3. LOGIC TÌM KIẾM ---

def search_data(query_type, params):
    conn = init_data_db()
    real_cols = get_display_columns()
    if not real_cols: return pd.DataFrame()
    
    select_cols_str = ", ".join([f'"{c}"' for c in real_cols]) # Quote tên cột để tránh lỗi SQL
    
    try:
        if query_type == 'ai':
            # Tìm kiếm thông minh trên cột Master
            keyword = clean_text(params)
            if not keyword: return pd.DataFrame()
            
            sql = f'SELECT {select_cols_str} FROM bhxh WHERE master_search_idx LIKE ? LIMIT 50'
            return pd.read_sql_query(sql, conn, params=(f'%{keyword}%',))
            
        elif query_type == 'manual':
            # Tìm kiếm chính xác trên từng cột
            conditions = []
            values = []
            for col, val in params.items():
                if val and val.strip():
                    clean_val = clean_text(val)
                    # Tìm trên cột index tương ứng (vd: idx_hoten)
                    # Cần đảm bảo tên cột trong params khớp với tên cột trong DB (đã lowercase)
                    db_col_idx = f"idx_{unidecode.unidecode(col).strip().replace(' ', '_').lower()}"
                    
                    conditions.append(f"{db_col_idx} LIKE ?")
                    values.append(f'%{clean_val}%')
            
            if not conditions: return pd.DataFrame()
            
            where_clause = " AND ".join(conditions)
            sql = f'SELECT {select_cols_str} FROM bhxh WHERE {where_clause} LIMIT 50'
            return pd.read_sql_query(sql, conn, params=tuple(values))
            
    except Exception as e:
        st.error(f"Lỗi tìm kiếm: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

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
                    st.rerun()
                else: st.error("Sai thông tin!")

def render_search_ai(is_ready):
    st.subheader("🤖 Tra Cứu Thông Minh (AI)")
    st.caption("Nhập bất kỳ thông tin nào: Tên viết liền, ngày sinh, số thẻ...")
    
    q = st.text_input("Từ khóa:", placeholder="vd: nguyenvana 1990")
    if q:
        df = search_data('ai', q)
        if not df.empty:
            st.success(f"Tìm thấy {len(df)} kết quả.")
            st.dataframe(df, use_container_width=True)
            
            if len(df) == 1 and is_ready:
                st.markdown("---")
                with st.spinner("AI đang phân tích..."):
                    try:
                        model = genai.GenerativeModel('gemini-pro')
                        prompt = f"Dữ liệu BHXH: {df.iloc[0].to_dict()}. Tóm tắt và tư vấn quyền lợi ngắn gọn bằng tiếng Việt."
                        res = model.generate_content(prompt)
                        st.info(res.text)
                    except: st.warning("Lỗi kết nối AI")
        else: st.warning("Không tìm thấy.")

def render_search_manual(cols):
    st.subheader("📋 Tra Cứu Chính Xác")
    st.caption("Nhập thông tin vào các ô bên dưới (không cần dấu).")
    
    with st.expander("Chọn trường tìm kiếm", expanded=True):
        # Gợi ý các cột quan trọng
        default = [c for c in cols if any(x in c for x in ['ten', 'sinh', 'ma', 'so'])]
        if not default: default = cols[:3]
        selected = st.multiselect("Cột:", cols, default=default)
    
    inputs = {}
    if selected:
        c = st.columns(len(selected))
        for i, col in enumerate(selected):
            inputs[col] = c[i].text_input(f"Nhập {col}")
    
    if st.button("🔍 Tìm kiếm", type="primary"):
        df = search_data('manual', inputs)
        if not df.empty:
            st.success(f"Tìm thấy {len(df)} kết quả.")
            st.dataframe(df, use_container_width=True)
        else: st.warning("Không tìm thấy.")

def render_admin():
    st.header("Quản Trị")
    conn = init_user_db()
    t1, t2 = st.tabs(["User", "Logs"])
    
    with t1:
        c1, c2 = st.columns(2)
        with c1:
            with st.form("add"):
                u = st.text_input("User mới")
                p = st.text_input("Pass", type="password")
                r = st.selectbox("Role", ["user", "admin"])
                if st.form_submit_button("Tạo"):
                    try:
                        conn.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r))
                        conn.commit()
                        st.success(f"Đã tạo {u}")
                        time.sleep(0.5); st.rerun()
                    except: st.error("Trùng tên!")
        with c2:
            users = [x[0] for x in conn.execute("SELECT username FROM users WHERE username != 'admin'").fetchall()]
            if users:
                ud = st.selectbox("Xóa user", users)
                if st.button("Xóa"):
                    conn.execute("DELETE FROM users WHERE username=?", (ud,))
                    conn.commit()
                    st.rerun()
            
        st.dataframe(pd.read_sql("SELECT username, role FROM users", conn), use_container_width=True)

    with t2:
        if st.button("Xóa Logs"):
            conn.execute("DELETE FROM logs")
            conn.commit()
            st.rerun()
        st.dataframe(pd.read_sql("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 50", conn), use_container_width=True)
    conn.close()

def main():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    
    # QUAN TRỌNG: Kiểm tra và nạp dữ liệu trước khi làm bất cứ gì khác
    data_ok, msg = import_excel_to_sqlite()
    if not data_ok:
        st.error(msg)
        if st.button("Thử nạp lại dữ liệu (Xóa cache)"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()
        return

    if not st.session_state['logged_in']:
        render_login()
    else:
        with st.sidebar:
            st.title(f"Hi, {st.session_state['username']}")
            
            # API Key Config
            key = st.text_input("API Key (AI)", type="password", value=st.session_state.get('user_api_key',''))
            if key: st.session_state['user_api_key'] = key
            
            st.divider()
            if 'page' not in st.session_state: st.session_state['page'] = 'ai'
            
            if st.button("🤖 Tra cứu AI"): st.session_state['page'] = 'ai'
            if st.button("📋 Tra cứu Thủ công"): st.session_state['page'] = 'manual'
            if st.session_state['role'] == 'admin':
                if st.button("🛠️ Quản trị"): st.session_state['page'] = 'admin'
            
            st.divider()
            if st.button("Đăng xuất"):
                st.session_state['logged_in'] = False
                st.rerun()

        # Main Content
        cols = get_display_columns()
        is_ai = configure_gemini()
        
        if st.session_state['page'] == 'ai': render_search_ai(is_ai)
        elif st.session_state['page'] == 'manual': render_search_manual(cols)
        elif st.session_state['page'] == 'admin': render_admin()

if __name__ == '__main__':
    init_user_db()
    main()
