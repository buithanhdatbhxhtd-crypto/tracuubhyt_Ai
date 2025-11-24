import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import datetime
import unidecode
import google.generativeai as genai
import time

# --- CẤU HÌNH ỨNG DỤNG ---
st.set_page_config(
    page_title="Hệ thống BHXH Thông Minh",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Tên file dữ liệu (Cần đảm bảo file này nằm cùng thư mục)
DATA_FILE = 'aaa.xlsb'

# --- 1. CÁC HÀM XỬ LÝ HỆ THỐNG & DATABASE ---
def init_db():
    """Khởi tạo cơ sở dữ liệu user và log"""
    conn = sqlite3.connect('users.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (timestamp TEXT, username TEXT, action TEXT, details TEXT)''')
    
    # Tạo admin mặc định (pass: admin123)
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
        conn = init_db()
        c = conn.cursor()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO logs VALUES (?, ?, ?, ?)", (ts, username, action, details))
        conn.commit()
        conn.close()
    except:
        pass

def configure_gemini():
    """Cấu hình API Key an toàn"""
    api_key = None
    # 1. Thử lấy từ Secrets (Streamlit Cloud)
    try:
        api_key = st.secrets["GOOGLE_API_KEY"]
    except:
        pass
    
    # 2. Nếu không có, lấy từ Session State (Người dùng nhập tay)
    if not api_key:
        api_key = st.session_state.get('user_api_key', '')

    if api_key:
        genai.configure(api_key=api_key)
        return True
    return False

# --- 2. XỬ LÝ DỮ LIỆU (CORE OPTIMIZATION) ---
@st.cache_data(show_spinner="Đang khởi tạo dữ liệu hệ thống...", ttl=3600)
def load_data_master():
    """
    Hàm nạp dữ liệu chỉ chạy 1 lần duy nhất.
    Tạo sẵn các cột chỉ mục (Index) để tìm kiếm siêu tốc.
    """
    try:
        start_time = time.time()
        # Đọc file với engine pyxlsb (nhanh hơn openpyxl cho file binary)
        df = pd.read_excel(DATA_FILE, engine='pyxlsb')
        
        # Chuẩn hóa tên cột (xóa khoảng trắng đầu cuối)
        df.columns = [str(c).strip() for c in df.columns]
        
        # Chuyển đổi dữ liệu sang string và xử lý NaN
        df = df.astype(str)
        df.replace(['nan', 'None', 'NaT'], '', inplace=True)

        # --- TỐI ƯU TÌM KIẾM ---
        # 1. Tạo bản sao không dấu cho TẤT CẢ các cột để tìm thủ công chính xác
        # Ví dụ: Cột 'hoTen' -> tạo thêm 'idx_hoTen' (chữ thường, không dấu)
        for col in df.columns:
            df[f'idx_{col}'] = df[col].apply(lambda x: unidecode.unidecode(x).lower() if x else '')

        # 2. Tạo một cột "Siêu chỉ mục" (Master Index) cho tìm kiếm AI/Tổng hợp
        # Gộp tất cả thông tin của 1 dòng thành 1 chuỗi dài không dấu
        df['Master_Search'] = df[[c for c in df.columns if not c.startswith('idx_')]].apply(
            lambda row: ' '.join(row.values.astype(str)), axis=1
        )
        df['Master_Search'] = df['Master_Search'].apply(lambda x: unidecode.unidecode(x).lower())

        return df, f"Đã nạp {len(df)} dòng dữ liệu trong {round(time.time() - start_time, 2)} giây."
    
    except FileNotFoundError:
        return None, "Lỗi: Không tìm thấy file dữ liệu."
    except Exception as e:
        return None, f"Lỗi nghiêm trọng: {str(e)}"

# --- 3. CÁC HÀM TRA CỨU ---

def manual_search_logic(df, filters):
    """
    Logic tra cứu thủ công chính xác theo cột
    filters: Dictionary { 'Tên cột gốc': 'Giá trị tìm kiếm' }
    """
    if not filters:
        return pd.DataFrame()
    
    # Bắt đầu với toàn bộ dữ liệu
    result_df = df.copy()
    
    for col, value in filters.items():
        if value:
            # Chuẩn hóa từ khóa tìm kiếm
            value_norm = unidecode.unidecode(value).lower().strip()
            # Tìm kiếm trên cột index tương ứng (đã tạo ở bước load_data)
            idx_col = f'idx_{col}'
            # Dùng str.contains để tìm chuỗi con (vectorized - rất nhanh)
            result_df = result_df[result_df[idx_col].str.contains(value_norm, na=False)]
            
            # Nếu kết quả rỗng thì dừng sớm
            if result_df.empty:
                break
    
    return result_df

def ai_search_logic(df, keyword):
    """Logic tra cứu tổng hợp (AI Search)"""
    if not keyword:
        return pd.DataFrame()
    
    keyword_norm = unidecode.unidecode(keyword).lower().strip()
    # Tìm trên cột Master_Search
    return df[df['Master_Search'].str.contains(keyword_norm, na=False)]

# --- 4. GIAO DIỆN NGƯỜI DÙNG ---

def render_login():
    """Hàm hiển thị màn hình đăng nhập"""
    st.markdown("<h2 style='text-align: center;'>🔐 Đăng Nhập Hệ Thống BHXH</h2>", unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập")
            password = st.text_input("Mật khẩu", type='password')
            submitted = st.form_submit_button("Đăng nhập", use_container_width=True)
            
            if submitted:
                conn = init_db()
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
                    st.error("Sai tên đăng nhập hoặc mật khẩu.")

def render_manual_search(df, cols_to_show):
    st.subheader("📋 Tra Cứu Thủ Công (Chính Xác)")
    st.caption("Chọn cột bạn muốn tìm kiếm, nhập thông tin (có dấu hoặc không dấu đều được).")
    
    # Chọn các cột muốn dùng để lọc
    with st.expander("⚙️ Cấu hình bộ lọc", expanded=True):
        # Gợi ý một số cột phổ biến nếu có
        default_options = []
        for c in ['hoten', 'ngaysinh', 'socmnd', 'cccd', 'mabhxh', 'mathe']:
            for col in cols_to_show:
                if c in unidecode.unidecode(col).lower():
                    default_options.append(col)
        
        if not default_options: default_options = cols_to_show[:3] # Mặc định lấy 3 cột đầu nếu ko khớp

        selected_filters = st.multiselect("Chọn tiêu chí tìm kiếm:", cols_to_show, default=default_options)

    # Tạo các ô input dựa trên cột đã chọn
    user_inputs = {}
    if selected_filters:
        cols = st.columns(len(selected_filters))
        for i, col_name in enumerate(selected_filters):
            user_inputs[col_name] = cols[i].text_input(f"Nhập {col_name}", key=f"search_{col_name}")

    # Nút tìm kiếm
    if st.button("🔍 Tìm kiếm thủ công", type="primary"):
        # Lọc bỏ các input rỗng
        valid_filters = {k: v for k, v in user_inputs.items() if v.strip() != ""}
        
        if valid_filters:
            results = manual_search_logic(df, valid_filters)
            st.success(f"Tìm thấy **{len(results)}** kết quả.")
            if not results.empty:
                # Chỉ hiện các cột gốc, không hiện cột index
                st.dataframe(results[cols_to_show], use_container_width=True)
                log_action(st.session_state['username'], "Manual Search", str(valid_filters))
        else:
            st.warning("Vui lòng nhập ít nhất một thông tin để tìm kiếm.")

def render_ai_search(df, cols_to_show, is_ai_ready):
    st.subheader("🤖 Tra Cứu Thông Minh & Trợ Lý AI")
    st.caption("Nhập bất kỳ thông tin nào (Tên, ngày sinh liền mạch, số thẻ...) vào ô bên dưới.")
    
    query = st.text_input("Nhập từ khóa tìm kiếm:", placeholder="Ví dụ: nguyen van a 1990, hoặc số CMND...")
    
    if query:
        results = ai_search_logic(df, query)
        count = len(results)
        
        if count > 0:
            st.success(f"Hệ thống tìm thấy **{count}** hồ sơ khớp với '{query}'")
            st.dataframe(results[cols_to_show], use_container_width=True)
            
            # Phần AI phân tích (Chỉ hiện khi tìm thấy 1-3 kết quả để tiết kiệm token)
            if count <= 3:
                st.markdown("---")
                st.markdown("### 💡 Phân tích từ AI")
                
                if not is_ai_ready:
                    st.warning("Chưa nhập API Key. Vui lòng nhập key ở thanh bên trái để dùng tính năng phân tích.")
                else:
                    for idx, row in results[cols_to_show].iterrows():
                        with st.expander(f"Phân tích hồ sơ: {row.iloc[0]} - {row.iloc[1]}", expanded=True):
                            with st.spinner("AI đang đọc dữ liệu..."):
                                try:
                                    model = genai.GenerativeModel('gemini-pro')
                                    prompt = f"""
                                    Dữ liệu BHXH: {row.to_dict()}
                                    Yêu cầu:
                                    1. Tóm tắt thông tin người này (Họ tên, tuổi, nơi ĐKCB).
                                    2. Kiểm tra các mã thẻ/mức hưởng và giải thích quyền lợi ngắn gọn.
                                    3. Trả lời bằng tiếng Việt, văn phong chuyên nghiệp.
                                    """
                                    response = model.generate_content(prompt)
                                    st.write(response.text)
                                except Exception as e:
                                    st.error(f"Lỗi AI: {e}")
            log_action(st.session_state['username'], "AI Search", query)
        else:
            st.warning("Không tìm thấy dữ liệu phù hợp.")

def render_admin():
    st.header("Quản Trị Hệ Thống")
    conn = init_db()
    
    tab1, tab2 = st.tabs(["Người dùng", "Nhật ký hoạt động"])
    
    with tab1:
        c = conn.cursor()
        c.execute("SELECT username, role FROM users")
        st.dataframe(pd.DataFrame(c.fetchall(), columns=['User', 'Role']), use_container_width=True)
        
        with st.form("new_user"):
            st.write("Thêm người dùng mới")
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            r = st.selectbox("Role", ["user", "admin"])
            if st.form_submit_button("Tạo"):
                try:
                    c.execute("INSERT INTO users VALUES (?,?,?)", (u, make_hashes(p), r))
                    conn.commit()
                    st.success("Tạo thành công!")
                    st.rerun()
                except:
                    st.error("Tên đăng nhập đã tồn tại.")
                    
    with tab2:
        if st.button("Xóa nhật ký"):
            c = conn.cursor()
            c.execute("DELETE FROM logs")
            conn.commit()
            st.rerun()
        
        c = conn.cursor()
        c.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100")
        st.dataframe(pd.DataFrame(c.fetchall(), columns=['Time', 'User', 'Action', 'Details']), use_container_width=True)
    
    conn.close()

# --- 5. HÀM MAIN (ĐIỀU HƯỚNG) ---
def main():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        render_login()  # <-- ĐÃ SỬA LẠI TÊN HÀM CHO ĐÚNG
    else:
        # --- SIDEBAR ---
        with st.sidebar:
            st.image("https://cdn-icons-png.flaticon.com/512/2303/2303512.png", width=50)
            st.title("Menu Điều Khiển")
            st.write(f"Xin chào, **{st.session_state['username']}**")
            
            # Cấu hình API Key
            with st.popover("Cấu hình AI Key"):
                key = st.text_input("Nhập Google Gemini API Key", type="password", value=st.session_state.get('user_api_key',''))
                if key:
                    st.session_state['user_api_key'] = key
                    st.success("Đã lưu key!")
            
            st.divider()
            
            page = st.radio("Chọn chức năng:", 
                            ["Tra cứu Thủ công", "Tra cứu AI", "Quản trị"] if st.session_state['role'] == 'admin' 
                            else ["Tra cứu Thủ công", "Tra cứu AI"])
            
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                log_action(st.session_state['username'], "Logout")
                st.session_state['logged_in'] = False
                st.rerun()

        # --- NẠP DỮ LIỆU ---
        df, status_msg = load_data_master()
        
        if df is None:
            st.error(f"⚠️ {status_msg}. Vui lòng kiểm tra file 'aaa.xlsb'.")
            return

        # Lấy danh sách cột gốc để hiển thị (loại bỏ các cột index ẩn)
        cols_to_show = [c for c in df.columns if not c.startswith('idx_') and c != 'Master_Search']
        is_ai_ready = configure_gemini()

        # --- ROUTING ---
        if page == "Tra cứu Thủ công":
            render_manual_search(df, cols_to_show)
        elif page == "Tra cứu AI":
            render_ai_search(df, cols_to_show, is_ai_ready)
        elif page == "Quản trị":
            render_admin()

if __name__ == '__main__':
    init_db()
    main()
