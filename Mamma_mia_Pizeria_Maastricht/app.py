from flask import Flask, render_template, request, jsonify, session
from models import PizzaModel
import json
from functools import wraps
import re
import os
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = 'pizza_secret_key_2024'

model = PizzaModel()

# Set timezone to Europe/Amsterdam 
os.environ['TZ'] = 'Europe/Amsterdam'
try:
    import time
    time.tzset()
    print("Timezone set to Europe/Amsterdam")
except (ImportError, AttributeError):
    print("Note: time.tzset() not available on this system, using default timezone")

def get_local_time():
    """Get current time with Amsterdam timezone adjustment"""
    try:
        
        return datetime.now()
    except:
        # Fallback: use UTC + Amsterdam offset approximation
        utc_now = datetime.utcnow()
        
        
        return utc_now + timedelta(hours=1)

@app.before_request
def before_request():
    """Set up timezone context for each request"""
    # Store current local time in session for reference
    session['current_time'] = get_local_time().isoformat()
    pass

def validate_email(email):
    """Basic email validation"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_phone(phone):
    """Basic phone validation"""
    # Remove common separators and check if it's mostly digits
    cleaned = re.sub(r'[\s\-\(\)\+]', '', phone)
    return cleaned.isdigit() and len(cleaned) >= 10

def staff_required(f):
    """Decorator for staff-only endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
    
        # Simple check for staff access in session
        if not session.get('is_staff'):
            return jsonify({'error': 'Staff access required'}), 403
        return f(*args, **kwargs)
    return decorated_function

# Applying security headers
@app.after_request
def apply_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    return response

# Enhanced customer validation
@app.route('/api/validate/customer_enhanced', methods=['POST'])
def validate_customer_enhanced():
    """Enhanced customer data validation"""
    try:
        data = request.get_json()
        
        errors = []
        
        # Email validation
        if not validate_email(data.get('email', '')):
            errors.append('Invalid email format')
        
        # Phone validation
        if not validate_phone(data.get('phone', '')):
            errors.append('Invalid phone number format')
        
        # Age validation
        birth_date = datetime.strptime(data['birth_date'], '%Y-%m-%d')
        min_age_date = get_local_time() - timedelta(days=365*13)
        
        if birth_date > min_age_date:
            errors.append('Customer must be at least 13 years old')
        
        # Postal code validation
        postal_code = data.get('postal_code', '')
        if not postal_code or len(postal_code) < 4:
            errors.append('Invalid postal code')
        
        if errors:
            return jsonify({'valid': False, 'errors': errors})
        
        return jsonify({'valid': True})
        
    except Exception as e:
        return jsonify({'valid': False, 'errors': [str(e)]})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/menu')
def menu():
    menu_data = model.get_menu()
    return render_template('menu.html', menu=menu_data)

@app.route('/order')
def order():
    menu_data = model.get_menu()
    return render_template('order.html', menu=menu_data)

@app.route('/staff')
def staff():
    return render_template('staff.html')

@app.route('/reports')
def reports():
    return render_template('reports.html')

@app.route('/delivery')
def delivery():
    return render_template('delivery.html')

@app.route('/api/place_order', methods=['POST'])
def place_order():
    try:
        data = request.get_json()
        
        customer_info = {
            'name': data['customer_name'],
            'email': data['customer_email'],
            'phone': data['customer_phone'],
            'address': data['customer_address'],
            'postal_code': data['customer_postal'],
            'birth_date': data['customer_birthdate'],
            'gender': data['customer_gender']
        }
        
        items = data['items']
        discount_code = data.get('discount_code')
        
        result = model.place_order(customer_info, items, discount_code)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/staff/reports')
def get_reports():
    try:
        reports = model.get_staff_reports()
        return jsonify(reports)
    except Exception as e:
        print(f"Error getting reports: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/staff/reports/revenue')
def get_revenue_reports():
    period = request.args.get('period', 'today')
    reports = model.get_revenue_reports(period)
    return jsonify(reports)

@app.route('/api/staff/order/<int:order_id>')
def get_order_details(order_id):
    try:
        order_data = model.get_order_details(order_id)
        if order_data:
            return jsonify(order_data)
        else:
            return jsonify({'error': 'Order not found'}), 404
    except Exception as e:
        print(f"Error getting order details: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/delivery/dashboard')
def get_delivery_dashboard():
    try:
        dashboard_data = model.get_delivery_dashboard()
        return jsonify(dashboard_data)
    except Exception as e:
        print(f"Error getting delivery dashboard: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/delivery/track/<int:order_id>')
def track_delivery(order_id):
    try:
        tracking_info = model.get_delivery_tracking(order_id)
        if tracking_info:
            return jsonify(tracking_info)
        else:
            return jsonify({'error': 'Order not found'}), 404
    except Exception as e:
        print(f"Error tracking delivery: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/delivery/update_status', methods=['POST'])
def update_delivery_status():
    try:
        data = request.get_json()
        order_id = data['order_id']
        status = data['status']
        notes = data.get('delivery_notes')
        
        success = model.update_delivery_status(order_id, status, notes)
        
        return jsonify({'success': success})
        
    except Exception as e:
        print(f"Error updating delivery status: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/delivery/available_drivers')
def get_available_drivers():
    postal_code = request.args.get('postal_code', '6211')
    available_drivers = model.get_available_delivery_persons(postal_code)
    return jsonify(available_drivers)

@app.route('/api/check_discount', methods=['POST'])
def check_discount():
    data = request.get_json()
    code = data.get('code')
    
    # Simple validation - in real app, check database
    valid_codes = ['WELCOME10', 'PIZZALOVER', 'SAVE20', 'MAASTRICHT25']
    if code in valid_codes:
        return jsonify({'valid': True, 'message': f'Discount code applied!'})
    else:
        return jsonify({'valid': False, 'message': 'Invalid discount code'})
    
@app.route('/api/test/constraints')
def test_constraints():
    """Test database constraints"""
    tests = model.test_constraints()
    return jsonify({'tests': tests})

@app.route('/api/validate/customer', methods=['POST'])
def validate_customer():
    """Validate customer data before order placement"""
    try:
        data = request.get_json()
        
        # Check age requirement
        birth_date = datetime.strptime(data['birth_date'], '%Y-%m-%d')
        min_age_date = get_local_time() - timedelta(days=365*13)  # 13 years ago
        
        if birth_date > min_age_date:
            return jsonify({
                'valid': False,
                'error': 'Customer must be at least 13 years old'
            })
        
        return jsonify({'valid': True})
        
    except Exception as e:
        return jsonify({'valid': False, 'error': str(e)})

@app.route('/api/order/<int:order_id>/cancel', methods=['POST'])
def cancel_order(order_id):
    """Cancel an order if within 5 minutes (staff can cancel anytime)"""
    try:
        is_staff = request.headers.get('X-Staff-Access') == 'true' or session.get('is_staff')
        result = model.cancel_order(order_id, is_staff=is_staff)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/order/<int:order_id>/cancellation_status')
def get_cancellation_status(order_id):
    """Check if an order can be cancelled"""
    try:
        status = model.get_order_cancellation_status(order_id)
        if status:
            return jsonify(status)
        else:
            return jsonify({'error': 'Order not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/sales')
def get_sales_analytics():
    """Get sales analytics for different time periods"""
    period = request.args.get('period', 'month')
    analytics = model.get_sales_analytics(period)
    return jsonify(analytics)

@app.route('/api/test/generate_data', methods=['POST'])
def generate_test_data():
    """Generate test data for development"""
    try:
        num_orders = request.json.get('num_orders', 20)
        result = model.generate_test_data(num_orders)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/performance/metrics')
def get_performance_metrics():
    """Get performance metrics"""
    try:
        metrics = model.get_performance_metrics()
        return jsonify(metrics)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/inventory/usage')
def get_inventory_usage():
    """Get ingredient usage analytics"""
    conn = model.db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM ingredient_usage ORDER BY total_used DESC')
    usage_data = cursor.fetchall()
    
    conn.close()
    
    return jsonify([
        {
            'ingredient_id': row[0],
            'name': row[1],
            'cost': float(row[2]),
            'total_used': row[3],
            'total_cost': float(row[4])
        } for row in usage_data
    ])

@app.route('/api/customers/loyalty_tiers')
def get_loyalty_tiers():
    """Get customer loyalty tiers"""
    conn = model.db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM customer_loyalty_tiers ORDER BY total_pizzas_ordered DESC')
    loyalty_data = cursor.fetchall()
    
    conn.close()
    
    return jsonify([
        {
            'customer_id': row[0],
            'name': row[1],
            'total_pizzas': row[2],
            'loyalty_tier': row[3],
            'discount_percent': row[4]
        } for row in loyalty_data
    ])

# Enhanced discount validation with database check
@app.route('/api/check_discount_enhanced', methods=['POST'])
def check_discount_enhanced():
    """Enhanced discount code validation with database checks"""
    data = request.get_json()
    code = data.get('code')
    
    conn = model.db.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT code_id, discount_percent, is_used, expiry_date 
            FROM discount_codes 
            WHERE code = ? AND expiry_date >= date('now')
        ''', (code,))
        
        code_data = cursor.fetchone()
        
        if code_data:
            code_id, discount_percent, is_used, expiry_date = code_data
            
            if is_used:
                return jsonify({'valid': False, 'message': 'Discount code has already been used'})
            
            return jsonify({
                'valid': True, 
                'message': f'{discount_percent}% discount applied!',
                'discount_percent': discount_percent
            })
        else:
            return jsonify({'valid': False, 'message': 'Invalid or expired discount code'})
            
    except Exception as e:
        return jsonify({'valid': False, 'message': f'Error validating code: {str(e)}'})
    finally:
        conn.close()

@app.route('/api/test/create_current_order', methods=['POST'])
def create_current_order():
    """Create a test order with current timestamp"""
    try:
        conn = model.db.get_connection()
        cursor = conn.cursor()
        
        # Create a test order with current time
        cursor.execute('''
            INSERT INTO orders (customer_id, total_amount, status, order_date)
            VALUES (1, 25.99, 'Pending', datetime('now', 'localtime'))
        ''')
        
        order_id = cursor.lastrowid
        
        # Add a pizza item
        cursor.execute('''
            INSERT INTO order_items (order_id, item_type, item_id, quantity, price_at_time)
            VALUES (?, 'pizza', 1, 1, 12.99)
        ''', (order_id,))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'order_id': order_id,
            'message': f'Test order created with current time: {get_local_time().strftime("%Y-%m-%d %H:%M:%S")}'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/debug/time')
def debug_time():
    """Debug endpoint to check current time settings"""
    current_utc = datetime.utcnow()
    current_local = datetime.now()
    app_local = get_local_time()
    
    return jsonify({
        'utc_time': current_utc.strftime('%Y-%m-%d %H:%M:%S'),
        'system_local_time': current_local.strftime('%Y-%m-%d %H:%M:%S'),
        'app_local_time': app_local.strftime('%Y-%m-%d %H:%M:%S'),
        'timezone_env': os.environ.get('TZ', 'Not set'),
        'session_current_time': session.get('current_time', 'Not set')
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)