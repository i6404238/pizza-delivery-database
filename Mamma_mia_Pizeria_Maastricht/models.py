from database import Database
import datetime
from datetime import date, timedelta

class PizzaModel:
    def __init__(self):
        self.db = Database()
    
    def get_menu(self):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT item_type, item_id, name, price, size, is_vegetarian, is_vegan 
            FROM menu_view 
            ORDER BY item_type, name
        ''')
        menu_items = cursor.fetchall()
        
        # Organize by category
        menu = {
            'pizzas': [],
            'drinks': [],
            'desserts': []
        }
        
        for item in menu_items:
            item_dict = {
                'id': item[1],
                'name': item[2],
                'price': float(item[3]),
                'size': item[4],
                'is_vegetarian': bool(item[5]),
                'is_vegan': bool(item[6])
            }
            
            if item[0] == 'pizza':
                menu['pizzas'].append(item_dict)
            elif item[0] == 'drink':
                menu['drinks'].append(item_dict)
            elif item[0] == 'dessert':
                menu['desserts'].append(item_dict)
        
        conn.close()
        return menu
    
    def get_available_delivery_persons(self, postal_code):
        """Get available delivery persons for a specific postal code"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Check for drivers who:
        # 1. Cover this postal code
        # 2. Are available OR their last delivery was more than 30 minutes ago
        cursor.execute('''
            SELECT 
                dp.driver_id,
                dp.name,
                dp.phone,
                dp.vehicle_type,
                ac.area_name,
                ac.delivery_time_minutes,
                dp.last_delivery_time,
                CASE 
                    WHEN dp.last_delivery_time IS NULL THEN 1
                    WHEN datetime(dp.last_delivery_time) <= datetime('now', '-30 minutes') THEN 1
                    ELSE 0
                END as is_actually_available
            FROM delivery_persons dp
            JOIN area_coverage ac ON dp.driver_id = ac.driver_id
            WHERE ac.postal_code = ?
            AND (dp.is_available = 1 OR datetime(dp.last_delivery_time) <= datetime('now', '-30 minutes'))
            ORDER BY is_actually_available DESC, ac.delivery_time_minutes ASC
        ''', (postal_code,))
        
        available_drivers = cursor.fetchall()
        conn.close()
        
        return [
            {
                'driver_id': driver[0],
                'name': driver[1],
                'phone': driver[2],
                'vehicle_type': driver[3],
                'area_name': driver[4],
                'delivery_time_minutes': driver[5],
                'last_delivery_time': driver[6],
                'is_actually_available': bool(driver[7])
            }
            for driver in available_drivers
        ]
    
    def assign_delivery_person(self, postal_code, order_id):
        """Assign a delivery person to an order based on postal code"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get available delivery persons for this postal code
            available_drivers = self.get_available_delivery_persons(postal_code)
            
            if not available_drivers:
                print(f"No available drivers found for postal code: {postal_code}")
                return None
            
            # Select the first available driver (sorted by availability and delivery time)
            selected_driver = available_drivers[0]
            
            # Calculate estimated delivery time
            delivery_time_minutes = selected_driver['delivery_time_minutes']
            estimated_delivery = datetime.datetime.now() + timedelta(minutes=delivery_time_minutes)
            
            print(f"Assigning driver {selected_driver['name']} to order {order_id}")
            
            # Assign the driver to the order
            cursor.execute('''
                UPDATE orders 
                SET delivery_person_id = ?, 
                    estimated_delivery_time = ?,
                    status = 'Preparing'
                WHERE order_id = ?
            ''', (selected_driver['driver_id'], estimated_delivery.isoformat(), order_id))
            
            # Mark driver as unavailable
            cursor.execute('''
                UPDATE delivery_persons 
                SET is_available = 0 
                WHERE driver_id = ?
            ''', (selected_driver['driver_id'],))
            
            conn.commit()
            
            return {
                'driver_id': selected_driver['driver_id'],
                'driver_name': selected_driver['name'],
                'phone': selected_driver['phone'],
                'vehicle_type': selected_driver['vehicle_type'],
                'estimated_delivery_time': estimated_delivery,
                'delivery_time_minutes': delivery_time_minutes,
                'area_name': selected_driver['area_name']
            }
            
        except Exception as e:
            conn.rollback()
            print(f"Error assigning delivery person: {e}")
            return None
        finally:
            conn.close()
    
    def update_delivery_status(self, order_id, status, delivery_notes=None):
        """Update delivery status and handle driver availability"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            if status == 'Out for Delivery':
                # Get the order details to check if we need to assign a driver
                cursor.execute('''
                    SELECT delivery_person_id, customer_id FROM orders WHERE order_id = ?
                ''', (order_id,))
                result = cursor.fetchone()
                
                if result:
                    delivery_person_id, customer_id = result
                    
                    # If no delivery person assigned, assign one based on customer's postal code
                    if not delivery_person_id:
                        # Get customer's postal code
                        cursor.execute('SELECT postal_code FROM customers WHERE customer_id = ?', (customer_id,))
                        customer_result = cursor.fetchone()
                        if customer_result:
                            postal_code = customer_result[0]
                            delivery_assignment = self.assign_delivery_person(postal_code, order_id)
                            if delivery_assignment:
                                print(f"Assigned driver {delivery_assignment['driver_name']} to order {order_id}")
                    
                    # Mark driver as on delivery
                    cursor.execute('''
                        UPDATE delivery_persons 
                        SET is_available = 0 
                        WHERE driver_id = (SELECT delivery_person_id FROM orders WHERE order_id = ?)
                    ''', (order_id,))
            
            elif status == 'Delivered':
                # Get the order details
                cursor.execute('''
                    SELECT delivery_person_id FROM orders WHERE order_id = ?
                ''', (order_id,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    # Mark driver as unavailable and set last delivery time
                    cursor.execute('''
                        UPDATE delivery_persons 
                        SET is_available = 0, 
                            last_delivery_time = CURRENT_TIMESTAMP 
                        WHERE driver_id = ?
                    ''', (result[0],))
                    
                    # Set actual delivery time
                    cursor.execute('''
                        UPDATE orders 
                        SET actual_delivery_time = CURRENT_TIMESTAMP 
                        WHERE order_id = ?
                    ''', (order_id,))
            
            elif status == 'Preparing':
                # Driver might become available again if order is back to preparing
                cursor.execute('''
                    SELECT delivery_person_id FROM orders WHERE order_id = ?
                ''', (order_id,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    # Check if driver's last delivery was more than 30 minutes ago
                    cursor.execute('''
                        UPDATE delivery_persons 
                        SET is_available = CASE 
                            WHEN last_delivery_time IS NULL THEN 1
                            WHEN datetime(last_delivery_time) <= datetime('now', '-30 minutes') THEN 1
                            ELSE 0
                        END
                        WHERE driver_id = ?
                    ''', (result[0],))
            
            # Update order status
            cursor.execute('''
                UPDATE orders 
                SET status = ?, 
                    delivery_notes = COALESCE(?, delivery_notes)
                WHERE order_id = ?
            ''', (status, delivery_notes, order_id))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error updating delivery status: {e}")
            return False
        finally:
            conn.close()
    
    def get_delivery_tracking(self, order_id):
        """Get delivery tracking information for an order"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                o.order_id,
                c.name as customer_name,
                c.address,
                c.postal_code,
                o.status,
                o.delivery_person_id,
                dp.name as delivery_person_name,
                dp.phone as delivery_phone,
                dp.vehicle_type,
                ac.area_name,
                o.estimated_delivery_time,
                o.actual_delivery_time,
                o.order_date,
                o.delivery_notes
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN delivery_persons dp ON o.delivery_person_id = dp.driver_id
            LEFT JOIN area_coverage ac ON (dp.driver_id = ac.driver_id AND c.postal_code = ac.postal_code)
            WHERE o.order_id = ?
        ''', (order_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
        
        return {
            'order_id': result[0],
            'customer_name': result[1],
            'address': result[2],
            'postal_code': result[3],
            'status': result[4],
            'delivery_person_id': result[5],
            'delivery_person_name': result[6],
            'delivery_phone': result[7],
            'vehicle_type': result[8],
            'area_name': result[9],
            'estimated_delivery_time': result[10],
            'actual_delivery_time': result[11],
            'order_date': result[12],
            'delivery_notes': result[13]
        }
    
    def get_delivery_dashboard(self):
        """Get dashboard data for delivery management"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get available drivers
        cursor.execute('''
            SELECT 
                driver_id,
                name,
                phone,
                vehicle_type,
                current_location,
                last_delivery_time,
                is_available,
                CASE 
                    WHEN last_delivery_time IS NULL THEN 'Available'
                    WHEN datetime(last_delivery_time) > datetime('now', '-30 minutes') THEN 'Recently Delivered'
                    ELSE 'Available'
                END as availability_status
            FROM delivery_persons
            ORDER BY 
                CASE 
                    WHEN is_available = 1 THEN 1
                    WHEN datetime(last_delivery_time) <= datetime('now', '-30 minutes') THEN 1
                    ELSE 0
                END DESC,
                name
        ''')
        
        drivers = cursor.fetchall()
        
        # Get active deliveries
        cursor.execute('''
            SELECT 
                o.order_id,
                c.name as customer_name,
                c.address,
                c.postal_code,
                o.status,
                dp.name as delivery_person_name,
                o.estimated_delivery_time,
                dp.phone as delivery_phone,
                dp.vehicle_type
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN delivery_persons dp ON o.delivery_person_id = dp.driver_id
            WHERE o.status IN ('Preparing', 'Out for Delivery')
            ORDER BY o.estimated_delivery_time
        ''')
        
        active_deliveries = cursor.fetchall()
        
        conn.close()
        
        return {
            'drivers': [
                {
                    'driver_id': driver[0],
                    'name': driver[1],
                    'phone': driver[2],
                    'vehicle_type': driver[3],
                    'current_location': driver[4],
                    'last_delivery_time': driver[5],
                    'is_available': bool(driver[6]),
                    'availability_status': driver[7]
                }
                for driver in drivers
            ],
            'active_deliveries': [
                {
                    'order_id': delivery[0],
                    'customer_name': delivery[1],
                    'address': delivery[2],
                    'postal_code': delivery[3],
                    'status': delivery[4],
                    'delivery_person_name': delivery[5],
                    'estimated_delivery_time': delivery[6],
                    'delivery_phone': delivery[7],
                    'vehicle_type': delivery[8]
                }
                for delivery in active_deliveries
            ]
        }
    
    def place_order(self, customer_info, items, discount_code=None):
        """Place an order with full transaction support and constraint validation"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Start explicit transaction
            cursor.execute('BEGIN TRANSACTION')
            print("🚀 Starting order transaction...")
            
            # Validate at least one pizza is ordered
            pizza_count = sum(1 for item in items if item['type'] == 'pizza' and item['quantity'] > 0)
            if pizza_count == 0:
                raise ValueError("Order must contain at least one pizza")
            
            # Check if customer exists or create new with validation
            cursor.execute(
                'SELECT customer_id, total_pizzas_ordered, birth_date FROM customers WHERE email = ?',
                (customer_info['email'],)
            )
            customer = cursor.fetchone()
            
            if customer:
                customer_id = customer[0]
                total_pizzas = customer[1]
                birth_date = customer[2]
                print(f"📋 Existing customer found: {customer_id}")
            else:
                # Create new customer with validation
                print(f"👤 Creating new customer: {customer_info['name']}")
                cursor.execute('''
                    INSERT INTO customers (name, email, phone, address, postal_code, birth_date, gender)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    customer_info['name'],
                    customer_info['email'],
                    customer_info['phone'],
                    customer_info['address'],
                    customer_info['postal_code'],
                    customer_info['birth_date'],
                    customer_info['gender']
                ))
                customer_id = cursor.lastrowid
                total_pizzas = 0
                birth_date = customer_info['birth_date']
                print(f"✅ New customer created with ID: {customer_id}")
            
            # Calculate total amount with validation
            total_amount = 0
            pizza_count = 0
            
            for item in items:
                if item['type'] == 'pizza':
                    cursor.execute(
                        'SELECT final_price FROM pizza_prices WHERE pizza_id = ?',
                        (item['id'],)
                    )
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError(f"Invalid pizza ID: {item['id']}")
                    price = result[0]
                    pizza_count += item['quantity']
                elif item['type'] == 'drink':
                    cursor.execute(
                        'SELECT price FROM drinks WHERE drink_id = ?',
                        (item['id'],)
                    )
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError(f"Invalid drink ID: {item['id']}")
                    price = result[0]
                else:  # dessert
                    cursor.execute(
                        'SELECT price FROM desserts WHERE dessert_id = ?',
                        (item['id'],)
                    )
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError(f"Invalid dessert ID: {item['id']}")
                    price = result[0]
                
                # Validate quantity
                if item['quantity'] <= 0 or item['quantity'] > 20:
                    raise ValueError(f"Invalid quantity for item: {item['quantity']}")
                
                total_amount += price * item['quantity']
            
            print(f"💰 Calculated subtotal: ${total_amount:.2f}")
            
            # Apply discounts with validation
            discount_amount = 0
            free_items = []
            
            # Check loyalty discount (10% after 10 pizzas)
            if total_pizzas + pizza_count >= 10:
                loyalty_discount = total_amount * 0.10
                discount_amount += loyalty_discount
                print(f"🎁 Applied loyalty discount: ${loyalty_discount:.2f}")
            
            # Check birthday discount
            today = date.today().isoformat()
            if birth_date and today[5:] == birth_date[5:]:  # Same month and day
                # Find cheapest pizza
                cursor.execute('SELECT MIN(final_price) FROM pizza_prices')
                cheapest_pizza_price = cursor.fetchone()[0] or 0
                
                # Find cheapest drink
                cursor.execute('SELECT MIN(price) FROM drinks')
                cheapest_drink_price = cursor.fetchone()[0] or 0
                
                birthday_discount = cheapest_pizza_price + cheapest_drink_price
                discount_amount += birthday_discount
                free_items.extend(['Free Pizza', 'Free Drink'])
                print(f"🎂 Applied birthday discount: ${birthday_discount:.2f}")
            
            # Check discount code with validation
            if discount_code:
                print(f"🔍 Validating discount code: {discount_code}")
                cursor.execute('''
                    SELECT code_id, discount_percent, is_used, expiry_date 
                    FROM discount_codes 
                    WHERE code = ? AND expiry_date >= ?
                ''', (discount_code, today))
                
                code_data = cursor.fetchone()
                if code_data:
                    code_id, discount_percent, is_used, expiry_date = code_data
                    
                    if is_used:
                        raise ValueError("Discount code has already been used")
                    
                    if expiry_date < today:
                        raise ValueError("Discount code has expired")
                    
                    code_discount = total_amount * (discount_percent / 100)
                    discount_amount += code_discount
                    
                    # Mark code as used
                    cursor.execute(
                        'UPDATE discount_codes SET is_used = 1, used_by_customer_id = ? WHERE code_id = ?',
                        (customer_id, code_id)
                    )
                    print(f"🏷️ Applied discount code: {discount_percent}% = ${code_discount:.2f}")
                else:
                    raise ValueError("Invalid or expired discount code")
            
            # Ensure discount doesn't exceed total
            if discount_amount > total_amount:
                discount_amount = total_amount
            
            final_amount = total_amount - discount_amount
            
            if final_amount < 0:
                final_amount = 0
            
            print(f"💳 Final amount after discounts: ${final_amount:.2f}")
            
            # Create order
            cursor.execute('''
                INSERT INTO orders (customer_id, total_amount, discount_applied, status)
                VALUES (?, ?, ?, 'Pending')
            ''', (customer_id, final_amount, discount_amount))
            
            order_id = cursor.lastrowid
            print(f"📦 Order created with ID: {order_id}")
            
            # Add order items with validation
            for item in items:
                if item['type'] == 'pizza':
                    cursor.execute(
                        'SELECT final_price FROM pizza_prices WHERE pizza_id = ?',
                        (item['id'],)
                    )
                    price = cursor.fetchone()[0]
                elif item['type'] == 'drink':
                    cursor.execute(
                        'SELECT price FROM drinks WHERE drink_id = ?',
                        (item['id'],)
                    )
                    price = cursor.fetchone()[0]
                else:  # dessert
                    cursor.execute(
                        'SELECT price FROM desserts WHERE dessert_id = ?',
                        (item['id'],)
                    )
                    price = cursor.fetchone()[0]
                
                cursor.execute('''
                    INSERT INTO order_items (order_id, item_type, item_id, quantity, price_at_time)
                    VALUES (?, ?, ?, ?, ?)
                ''', (order_id, item['type'], item['id'], item['quantity'], price))
                print(f"➕ Added {item['type']} ID {item['id']} x {item['quantity']}")
            
            # Update customer's pizza count
            cursor.execute(
                'UPDATE customers SET total_pizzas_ordered = total_pizzas_ordered + ? WHERE customer_id = ?',
                (pizza_count, customer_id)
            )
            print(f"👤 Updated customer pizza count: +{pizza_count}")
            
            # Try to assign delivery person
            delivery_assignment = self.assign_delivery_person(customer_info['postal_code'], order_id)
            
            # Commit transaction
            conn.commit()
            print("✅ Order transaction committed successfully!")
            
            return {
                'success': True,
                'order_id': order_id,
                'total_amount': final_amount,
                'discount_amount': discount_amount,
                'free_items': free_items,
                'delivery_assignment': delivery_assignment
            }
            
        except Exception as e:
            # Rollback transaction on any error
            conn.rollback()
            print(f"❌ Order transaction failed: {str(e)}")
            print("🔄 Rolling back transaction...")
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def test_constraints(self):
        """Test various constraints to ensure they work properly"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        tests = []
        
        try:
            # Test 1: Try to create customer under 13 years old
            try:
                young_birthdate = (datetime.datetime.now() - timedelta(days=365*12)).strftime('%Y-%m-%d')  # 12 years old
                cursor.execute('''
                    INSERT INTO customers (name, email, phone, address, postal_code, birth_date, gender)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', ('Young Customer', 'young@test.com', '123456789', 'Test St', '6211', young_birthdate, 'Male'))
                tests.append(('Age Validation', 'FAILED - Should have rejected young customer'))
                conn.rollback()
            except Exception as e:
                if 'at least 13 years' in str(e):
                    tests.append(('Age Validation', 'PASSED'))
                else:
                    tests.append(('Age Validation', f'FAILED - Wrong error: {e}'))
            
            # Test 2: Try to add meat to vegetarian pizza
            try:
                cursor.execute('INSERT INTO pizza_ingredients (pizza_id, ingredient_id) VALUES (?, ?)', (1, 3))  # Margherita + Pepperoni
                tests.append(('Vegetarian Constraint', 'FAILED - Should have rejected meat in vegetarian pizza'))
                conn.rollback()
            except Exception as e:
                if 'non-vegetarian' in str(e):
                    tests.append(('Vegetarian Constraint', 'PASSED'))
                else:
                    tests.append(('Vegetarian Constraint', f'FAILED - Wrong error: {e}'))
            
            # Test 3: Try to use negative ingredient cost
            try:
                cursor.execute('INSERT INTO ingredients (name, cost, is_vegetarian, is_vegan) VALUES (?, ?, ?, ?)', 
                             ('Test Ingredient', -1.0, 1, 1))
                tests.append(('Positive Cost Constraint', 'FAILED - Should have rejected negative cost'))
                conn.rollback()
            except Exception as e:
                if 'CHECK' in str(e):
                    tests.append(('Positive Cost Constraint', 'PASSED'))
                else:
                    tests.append(('Positive Cost Constraint', f'FAILED - Wrong error: {e}'))
            
            # Test 4: Try to reuse discount code
            try:
                # First, use a discount code
                cursor.execute('UPDATE discount_codes SET is_used = 1, used_by_customer_id = 1 WHERE code_id = 1')
                
                # Try to use it again
                cursor.execute('UPDATE discount_codes SET is_used = 1, used_by_customer_id = 2 WHERE code_id = 1')
                tests.append(('Discount Code Reuse', 'FAILED - Should have prevented reuse'))
                conn.rollback()
            except Exception as e:
                if 'already been used' in str(e):
                    tests.append(('Discount Code Reuse', 'PASSED'))
                else:
                    tests.append(('Discount Code Reuse', f'FAILED - Wrong error: {e}'))
            
            # Test 5: Try to create order without pizza
            try:
                cursor.execute('INSERT INTO orders (customer_id, total_amount) VALUES (?, ?)', (1, 25.0))
                order_id = cursor.lastrowid
                cursor.execute('INSERT INTO order_items (order_id, item_type, item_id, quantity, price_at_time) VALUES (?, ?, ?, ?, ?)',
                             (order_id, 'drink', 1, 1, 2.5))
                tests.append(('Minimum Pizza Constraint', 'FAILED - Should have required at least one pizza'))
                conn.rollback()
            except Exception as e:
                if 'at least one pizza' in str(e):
                    tests.append(('Minimum Pizza Constraint', 'PASSED'))
                else:
                    tests.append(('Minimum Pizza Constraint', f'FAILED - Wrong error: {e}'))
            
        finally:
            conn.close()
        
        return tests
    
    def test_enhanced_constraints(self):
        """Test enhanced constraints and business rules"""
        conn = self.db.get_connection()
        cursor = conn.cursor()

        tests = []

        try:
            # Test 1: Order cancellation within 5 minutes
            try:
                # Create a test order
                cursor.execute('''
                    INSERT INTO orders (customer_id, total_amount, order_date)
                    VALUES (1, 25.0, datetime('now', '-10 minutes'))
                ''')
                order_id = cursor.lastrowid

                # Try to cancel (should fail)
                cursor.execute('UPDATE orders SET status = "Cancelled" WHERE order_id = ?', (order_id,))
                tests.append(('Cancellation Time Window', 'FAILED - Should have prevented late cancellation'))
                conn.rollback()
            except Exception as e:
                if '5 minutes' in str(e):
                    tests.append(('Cancellation Time Window', 'PASSED'))
                else:
                    tests.append(('Cancellation Time Window', f'FAILED - Wrong error: {e}'))

            # Test 2: Duplicate discount codes
            try:
                cursor.execute('''
                    INSERT INTO discount_codes (code, discount_percent, expiry_date)
                    VALUES (?, 15, date('now', '+365 days'))
                ''', ('TESTCODE',))

                cursor.execute('''
                    INSERT INTO discount_codes (code, discount_percent, expiry_date)
                    VALUES (?, 20, date('now', '+365 days'))
                ''', ('TESTCODE',))

                tests.append(('Unique Discount Codes', 'FAILED - Should have prevented duplicate codes'))
                conn.rollback()
            except Exception as e:
                if 'unique' in str(e).lower():
                    tests.append(('Unique Discount Codes', 'PASSED'))
                else:
                    tests.append(('Unique Discount Codes', f'FAILED - Wrong error: {e}'))

            # Test 3: Negative order total
            try:
                cursor.execute('INSERT INTO orders (customer_id, total_amount) VALUES (1, -10.0)')
                tests.append(('Positive Order Total', 'FAILED - Should have rejected negative total'))
                conn.rollback()
            except Exception as e:
                if 'CHECK' in str(e):
                    tests.append(('Positive Order Total', 'PASSED'))
                else:
                    tests.append(('Positive Order Total', f'FAILED - Wrong error: {e}'))

            # Test 4: Excessive order total
            try:
                cursor.execute('INSERT INTO orders (customer_id, total_amount) VALUES (1, 1500.0)')
                tests.append(('Reasonable Order Total', 'FAILED - Should have rejected excessive total'))
                conn.rollback()
            except Exception as e:
                if 'CHECK' in str(e):
                    tests.append(('Reasonable Order Total', 'PASSED'))
                else:
                    tests.append(('Reasonable Order Total', f'FAILED - Wrong error: {e}'))

        finally:
            conn.close()

        return tests

    # ========== STAFF REPORTS METHODS ==========
    
    def get_staff_reports(self):
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        reports = {}
        
        # Undelivered orders with delivery person info
        cursor.execute('''
            SELECT 
                o.order_id, 
                c.name, 
                o.order_date, 
                o.total_amount, 
                o.status,
                dp.name as delivery_person_name,
                dp.phone as delivery_phone,
                dp.vehicle_type
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN delivery_persons dp ON o.delivery_person_id = dp.driver_id
            WHERE o.status != 'Delivered'
            ORDER BY o.order_date
        ''')
        reports['undelivered_orders'] = cursor.fetchall()
        
        # Top 3 pizzas in last month
        cursor.execute('''
            SELECT p.name, SUM(oi.quantity) as total_sold
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            JOIN pizzas p ON oi.item_id = p.pizza_id
            WHERE oi.item_type = 'pizza' 
            AND o.order_date >= date('now', '-1 month')
            GROUP BY p.name
            ORDER BY total_sold DESC
            LIMIT 3
        ''')
        reports['top_pizzas'] = cursor.fetchall()
        
        # Earnings by gender
        cursor.execute('''
            SELECT c.gender, SUM(o.total_amount) as total_earnings
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY c.gender
        ''')
        reports['earnings_by_gender'] = cursor.fetchall()
        
        # Earnings by age group
        cursor.execute('''
            SELECT 
                CASE 
                    WHEN (strftime('%Y', 'now') - strftime('%Y', birth_date)) < 25 THEN 'Under 25'
                    WHEN (strftime('%Y', 'now') - strftime('%Y', birth_date)) BETWEEN 25 AND 40 THEN '25-40'
                    ELSE 'Over 40'
                END as age_group,
                SUM(o.total_amount) as total_earnings
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY age_group
        ''')
        reports['earnings_by_age'] = cursor.fetchall()
        
        # Earnings by postal code
        cursor.execute('''
            SELECT c.postal_code, SUM(o.total_amount) as total_earnings
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY c.postal_code
        ''')
        reports['earnings_by_postal'] = cursor.fetchall()
        
        # Customer insights - FIXED: Handle cases with no data
        cursor.execute('''
            SELECT name, total_pizzas_ordered 
            FROM customers 
            ORDER BY total_pizzas_ordered DESC 
            LIMIT 1
        ''')
        top_customer = cursor.fetchone()
        
        # Get total number of customers who ordered more than once
        cursor.execute('SELECT COUNT(*) FROM customers WHERE total_pizzas_ordered > 1')
        repeat_customers_result = cursor.fetchone()
        repeat_customers = repeat_customers_result[0] if repeat_customers_result else 0
        
        # FIXED: Check if we have any orders to calculate average - use the actual method
        avg_order_value = self.get_average_order_value()
        
        # If avg_order_value is 0, check if there are any orders at all
        if avg_order_value == 0:
            cursor.execute('SELECT COUNT(*) FROM orders')
            total_orders = cursor.fetchone()[0] or 0
            if total_orders == 0:
                # No orders yet, so average order value should be N/A
                avg_order_value_display = None
            else:
                # There are orders but average is 0 (shouldn't happen normally)
                avg_order_value_display = 0
        else:
            avg_order_value_display = avg_order_value
        
        reports['customer_insights'] = {
            'top_customer': top_customer[0] if top_customer else 'N/A',
            'avg_order_value': avg_order_value_display,  # This will be None if no orders
            'repeat_customers': repeat_customers
        }
        
        # Delivery performance - real data
        cursor.execute('''
            SELECT 
                ROUND(AVG((julianday(actual_delivery_time) - julianday(order_date)) * 24 * 60)) as avg_delivery_minutes,
                COUNT(*) as total_deliveries,
                SUM(CASE WHEN (julianday(actual_delivery_time) - julianday(order_date)) * 24 * 60 <= 45 THEN 1 ELSE 0 END) as on_time_deliveries
            FROM orders 
            WHERE actual_delivery_time IS NOT NULL
        ''')
        delivery_stats = cursor.fetchone()
        
        if delivery_stats and delivery_stats[1] and delivery_stats[1] > 0:
            avg_minutes = int(delivery_stats[0]) if delivery_stats[0] else 45
            on_time_rate = (delivery_stats[2] / delivery_stats[1] * 100) if delivery_stats[1] > 0 else 0
        else:
            avg_minutes = 45
            on_time_rate = 0
        
        # Get top driver - only if we have deliveries
        cursor.execute('''
            SELECT dp.name, COUNT(*) as delivery_count
            FROM orders o
            JOIN delivery_persons dp ON o.delivery_person_id = dp.driver_id
            WHERE o.actual_delivery_time IS NOT NULL
            GROUP BY dp.name
            ORDER BY delivery_count DESC
            LIMIT 1
        ''')
        top_driver = cursor.fetchone()
        
        reports['delivery_performance'] = {
            'avg_delivery_time': f'{avg_minutes} min',
            'on_time_rate': f'{on_time_rate:.0f}%' if on_time_rate > 0 else 'N/A',
            'top_driver': top_driver[0] if top_driver else 'N/A'
        }
        
        # Discount usage - real data
        cursor.execute('''
            SELECT SUM(discount_applied) as total_discounts
            FROM orders
            WHERE discount_applied > 0
        ''')
        total_discounts_result = cursor.fetchone()
        total_discounts = float(total_discounts_result[0]) if total_discounts_result and total_discounts_result[0] else 0
        
        cursor.execute('''
            SELECT COUNT(*) 
            FROM discount_codes 
            WHERE is_used = 1
        ''')
        used_codes_result = cursor.fetchone()
        used_codes = used_codes_result[0] if used_codes_result else 0
        
        # Count birthday offers used (orders with birthday discounts)
        cursor.execute('''
            SELECT COUNT(*) 
            FROM orders 
            WHERE discount_applied > 0 
            AND strftime('%m-%d', order_date) IN (
                SELECT strftime('%m-%d', birth_date) FROM customers
            )
        ''')
        birthday_offers_result = cursor.fetchone()
        birthday_offers = birthday_offers_result[0] if birthday_offers_result else 0
        
        reports['discount_usage'] = {
            'loyalty_discounts': total_discounts,
            'birthday_offers': birthday_offers,
            'promo_codes_used': used_codes
        }
        
        # Real-time stats
        reports['real_time_stats'] = {
            'pending_orders': len(reports['undelivered_orders']),
            'monthly_revenue': self.get_monthly_revenue(),
            'total_pizzas': self.get_monthly_pizza_count(),
            'active_customers': self.get_active_customer_count()
        }
        
        # Advanced reports - only show if we have data
        cursor.execute('''
            SELECT 
                strftime('%H', order_date) as hour,
                COUNT(*) as order_count
            FROM orders
            GROUP BY strftime('%H', order_date)
            ORDER BY order_count DESC
            LIMIT 3
        ''')
        peak_hours = cursor.fetchall()
        
        peak_times = []
        if peak_hours:
            for hour_data in peak_hours:
                hour = int(hour_data[0])
                orders_count = hour_data[1]
                peak_times.append({
                    'period': f'{hour}:00-{hour+2}:00',
                    'orders': orders_count
                })
        else:
            # Default data if no orders
            peak_times = [
                {'period': '6:00-8:00 PM', 'orders': 0},
                {'period': '12:00-2:00 PM', 'orders': 0},
                {'period': '7:00-9:00 PM', 'orders': 0}
            ]
        
        # Popular combinations
        cursor.execute('''
            SELECT 
                p.name as pizza_name,
                d.name as drink_name,
                COUNT(*) as combo_count
            FROM orders o
            JOIN order_items oi_pizza ON o.order_id = oi_pizza.order_id AND oi_pizza.item_type = 'pizza'
            JOIN order_items oi_drink ON o.order_id = oi_drink.order_id AND oi_drink.item_type = 'drink'
            JOIN pizzas p ON oi_pizza.item_id = p.pizza_id
            JOIN drinks d ON oi_drink.item_id = d.drink_id
            GROUP BY p.name, d.name
            ORDER BY combo_count DESC
            LIMIT 3
        ''')
        popular_combos = cursor.fetchall()
        
        combinations = []
        if popular_combos:
            for combo in popular_combos:
                combinations.append({
                    'items': f'{combo[0]} + {combo[1]}',
                    'count': combo[2]
                })
        else:
            combinations = [
                {'items': 'No combinations yet', 'count': 0}
            ]
        
        # Customer retention rate
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT customer_id) as total_customers,
                COUNT(DISTINCT CASE WHEN total_pizzas_ordered > 1 THEN customer_id END) as repeat_customers
            FROM customers
            WHERE total_pizzas_ordered > 0
        ''')
        retention_data = cursor.fetchone()
        
        if retention_data and retention_data[0] and retention_data[0] > 0:
            retention_rate = (retention_data[1] / retention_data[0]) * 100
        else:
            retention_rate = 0
        
        reports['advanced_reports'] = {
            'peak_times': peak_times,
            'popular_combinations': combinations,
            'customer_retention': {
                'rate': f'{retention_rate:.0f}%' if retention_rate > 0 else 'N/A'
            }
        }
        
        conn.close()
        return reports

    
    def get_revenue_reports(self, period='today'):
        """Get revenue reports filtered by time period"""
        # For now, return the same data as get_staff_reports
        # In a real implementation, this would filter by the specified period
        return self.get_staff_reports()
    
    def get_order_details(self, order_id):
        """Get detailed information about a specific order"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get order basic info with delivery person details
        cursor.execute('''
            SELECT 
                o.order_id, 
                c.name, 
                c.address, 
                c.phone, 
                o.total_amount, 
                o.status, 
                o.order_date,
                o.delivery_person_id,
                dp.name as delivery_person_name,
                dp.phone as delivery_phone,
                dp.vehicle_type,
                o.estimated_delivery_time,
                o.actual_delivery_time,
                ac.area_name
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN delivery_persons dp ON o.delivery_person_id = dp.driver_id
            LEFT JOIN area_coverage ac ON (dp.driver_id = ac.driver_id AND c.postal_code = ac.postal_code)
            WHERE o.order_id = ?
        ''', (order_id,))
        
        order_data = cursor.fetchone()
        
        if not order_data:
            return None
        
        # Get order items
        cursor.execute('''
            SELECT 
                CASE 
                    WHEN item_type = 'pizza' THEN (SELECT name FROM pizzas WHERE pizza_id = item_id)
                    WHEN item_type = 'drink' THEN (SELECT name FROM drinks WHERE drink_id = item_id)
                    WHEN item_type = 'dessert' THEN (SELECT name FROM desserts WHERE dessert_id = item_id)
                END as name,
                quantity,
                price_at_time
            FROM order_items
            WHERE order_id = ?
        ''', (order_id,))
        
        items = cursor.fetchall()
        
        conn.close()
        
        return {
            'order_id': order_data[0],
            'customer_name': order_data[1],
            'customer_address': order_data[2],
            'customer_phone': order_data[3],
            'total_amount': float(order_data[4]),
            'status': order_data[5],
            'order_date': order_data[6],
            'delivery_person_id': order_data[7],
            'delivery_person_name': order_data[8],
            'delivery_phone': order_data[9],
            'vehicle_type': order_data[10],
            'estimated_delivery_time': order_data[11],
            'actual_delivery_time': order_data[12],
            'area_name': order_data[13],
            'items': [{
                'name': item[0],
                'quantity': item[1],
                'price': float(item[2])
            } for item in items]
        }
    
    def get_average_order_value(self):
        """Calculate average order value - FIXED to return None when no orders"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # First check if there are any orders
        cursor.execute('SELECT COUNT(*) FROM orders')
        total_orders = cursor.fetchone()[0]
        
        if total_orders == 0:
            conn.close()
            return None  # Return None when no orders exist
        
        cursor.execute('SELECT AVG(total_amount) FROM orders WHERE total_amount > 0')
        result = cursor.fetchone()[0]
        conn.close()
        
        return float(result) if result else 0
    
    def get_repeat_customer_count(self):
        """Count customers who have ordered more than once"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM customers WHERE total_pizzas_ordered > 1')
        result = cursor.fetchone()[0]
        conn.close()
        
        return result if result else 0
    
    def get_monthly_revenue(self):
        """Calculate monthly revenue"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT SUM(total_amount) FROM orders 
            WHERE order_date >= date('now', '-1 month') AND total_amount > 0
        ''')
        result = cursor.fetchone()[0]
        conn.close()
        
        return float(result) if result else 0
    
    def get_monthly_pizza_count(self):
        """Count pizzas sold in the last month"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT SUM(oi.quantity) 
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE oi.item_type = 'pizza' 
            AND o.order_date >= date('now', '-1 month')
        ''')
        result = cursor.fetchone()[0]
        conn.close()
        
        return result if result else 0
    
    def get_active_customer_count(self):
        """Count active customers (ordered in last 3 months)"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(DISTINCT customer_id) 
            FROM orders 
            WHERE order_date >= date('now', '-3 month')
        ''')
        result = cursor.fetchone()[0]
        conn.close()
        
        return result if result else 0
    
    def cancel_order(self, order_id, is_staff=False):
        """Cancel an order if within 5 minutes (or anytime for staff)"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('BEGIN TRANSACTION')
            
            if not is_staff:
                # Check if order can be cancelled (5-minute window for customers)
                cursor.execute('''
                    SELECT can_cancel FROM order_cancellation_eligibility 
                    WHERE order_id = ?
                ''', (order_id,))
                
                result = cursor.fetchone()
                if not result or not result[0]:
                    raise ValueError("Order cannot be cancelled - 5 minute window has passed")
            
            # Get order details for refund processing
            cursor.execute('SELECT customer_id, total_amount FROM orders WHERE order_id = ?', (order_id,))
            order_info = cursor.fetchone()
            
            if not order_info:
                raise ValueError("Order not found")
            
            # Update order status
            cursor.execute('''
                UPDATE orders SET status = 'Cancelled' WHERE order_id = ?
            ''', (order_id,))
            
            # Free up delivery person if assigned
            cursor.execute('''
                UPDATE delivery_persons 
                SET is_available = 1 
                WHERE driver_id = (
                    SELECT delivery_person_id FROM orders WHERE order_id = ?
                )
            ''', (order_id,))
            
            # Log the cancellation
            cancellation_type = "staff" if is_staff else "customer"
            cursor.execute('''
                INSERT INTO order_cancellations (order_id, cancelled_by, cancellation_type, cancellation_time)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (order_id, "system", cancellation_type))
            
            conn.commit()
            return {
                'success': True, 
                'message': 'Order cancelled successfully',
                'refund_amount': float(order_info[1]) if order_info[1] else 0
            }
            
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def get_order_cancellation_status(self, order_id):
        """Check if an order can be cancelled"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT can_cancel, cancellation_deadline, status 
            FROM order_cancellation_eligibility 
            WHERE order_id = ?
        ''', (order_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'can_cancel': bool(result[0]),
                'cancellation_deadline': result[1],
                'current_status': result[2]
            }
        return None
    
    def get_sales_analytics(self, period='month'):
        """Get sales analytics for different time periods"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        period_map = {
            'today': '1 day',
            'week': '7 days',
            'month': '1 month',
            'quarter': '3 months',
            'year': '1 year'
        }
        
        interval = period_map.get(period, '1 month')
        
        # Revenue by day/week/month
        cursor.execute(f'''
            SELECT 
                date(order_date) as period,
                COUNT(*) as order_count,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value
            FROM orders 
            WHERE order_date >= datetime('now', '-{interval}')
            GROUP BY date(order_date)
            ORDER BY period DESC
        ''')
        
        revenue_trends = cursor.fetchall()
        
        # Top selling items
        cursor.execute(f'''
            SELECT 
                CASE 
                    WHEN oi.item_type = 'pizza' THEN (SELECT name FROM pizzas WHERE pizza_id = oi.item_id)
                    WHEN oi.item_type = 'drink' THEN (SELECT name FROM drinks WHERE drink_id = oi.item_id)
                    WHEN oi.item_type = 'dessert' THEN (SELECT name FROM desserts WHERE dessert_id = oi.item_id)
                END as item_name,
                oi.item_type,
                SUM(oi.quantity) as total_sold,
                SUM(oi.quantity * oi.price_at_time) as total_revenue
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date >= datetime('now', '-{interval}')
            GROUP BY oi.item_type, oi.item_id
            ORDER BY total_sold DESC
            LIMIT 10
        ''')
        
        top_items = cursor.fetchall()
        
        # Customer acquisition
        cursor.execute(f'''
            SELECT 
                strftime('%Y-%m', created_date) as signup_month,
                COUNT(*) as new_customers,
                SUM(total_pizzas_ordered) as pizzas_ordered
            FROM customers
            WHERE created_date >= datetime('now', '-{interval}')
            GROUP BY strftime('%Y-%m', created_date)
            ORDER BY signup_month DESC
        ''')
        
        customer_acquisition = cursor.fetchall()
        
        conn.close()
        
        return {
            'revenue_trends': [
                {
                    'period': row[0],
                    'order_count': row[1],
                    'total_revenue': float(row[2]) if row[2] else 0,
                    'avg_order_value': float(row[3]) if row[3] else 0
                } for row in revenue_trends
            ],
            'top_items': [
                {
                    'name': row[0],
                    'type': row[1],
                    'total_sold': row[2],
                    'total_revenue': float(row[3]) if row[3] else 0
                } for row in top_items
            ],
            'customer_acquisition': [
                {
                    'month': row[0],
                    'new_customers': row[1],
                    'pizzas_ordered': row[2]
                } for row in customer_acquisition
            ]
        }
    
    def generate_test_data(self, num_orders=20):
        """Generate realistic test data using Faker"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('BEGIN TRANSACTION')
            
            # Generate orders across different timeframes
            for i in range(num_orders):
                # Create customer or use existing
                if random.random() > 0.7:  # 30% chance new customer
                    customer_data = {
                        'name': self.fake.name(),
                        'email': self.fake.email(),
                        'phone': self.fake.phone_number(),
                        'address': self.fake.address(),
                        'postal_code': random.choice(['6211', '6212', '6217', '6221', '6215']),
                        'birth_date': self.fake.date_of_birth(minimum_age=13, maximum_age=80),
                        'gender': random.choice(['Male', 'Female', 'Other'])
                    }
                    
                    cursor.execute('''
                        INSERT INTO customers (name, email, phone, address, postal_code, birth_date, gender)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        customer_data['name'],
                        customer_data['email'],
                        customer_data['phone'],
                        customer_data['address'],
                        customer_data['postal_code'],
                        customer_data['birth_date'],
                        customer_data['gender']
                    ))
                    customer_id = cursor.lastrowid
                else:
                    # Use existing customer
                    cursor.execute('SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1')
                    customer_id = cursor.fetchone()[0]
                
                # Random order date in the past 90 days
                order_date = self.fake.date_time_between(start_date='-90d', end_date='now')
                
                # Create order
                total_amount = round(random.uniform(15, 75), 2)
                
                cursor.execute('''
                    INSERT INTO orders (customer_id, order_date, total_amount, status)
                    VALUES (?, ?, ?, ?)
                ''', (customer_id, order_date.isoformat(), total_amount, 'Delivered'))
                
                order_id = cursor.lastrowid
                
                # Add order items
                # At least one pizza
                cursor.execute('SELECT pizza_id FROM pizzas ORDER BY RANDOM() LIMIT 1')
                pizza_id = cursor.fetchone()[0]
                cursor.execute('SELECT final_price FROM pizza_prices WHERE pizza_id = ?', (pizza_id,))
                pizza_price = cursor.fetchone()[0]
                
                cursor.execute('''
                    INSERT INTO order_items (order_id, item_type, item_id, quantity, price_at_time)
                    VALUES (?, 'pizza', ?, ?, ?)
                ''', (order_id, pizza_id, random.randint(1, 3), pizza_price))
                
                # Possibly add drinks
                if random.random() > 0.3:
                    cursor.execute('SELECT drink_id, price FROM drinks ORDER BY RANDOM() LIMIT 1')
                    drink = cursor.fetchone()
                    cursor.execute('''
                        INSERT INTO order_items (order_id, item_type, item_id, quantity, price_at_time)
                        VALUES (?, 'drink', ?, ?, ?)
                    ''', (order_id, drink[0], random.randint(1, 2), drink[1]))
                
                # Possibly add desserts
                if random.random() > 0.5:
                    cursor.execute('SELECT dessert_id, price FROM desserts ORDER BY RANDOM() LIMIT 1')
                    dessert = cursor.fetchone()
                    cursor.execute('''
                        INSERT INTO order_items (order_id, item_type, item_id, quantity, price_at_time)
                        VALUES (?, 'dessert', ?, ?, ?)
                    ''', (order_id, dessert[0], 1, dessert[1]))
                
                # Update customer pizza count
                cursor.execute('''
                    UPDATE customers 
                    SET total_pizzas_ordered = total_pizzas_ordered + ? 
                    WHERE customer_id = ?
                ''', (random.randint(1, 3), customer_id))
            
            conn.commit()
            return {'success': True, 'message': f'Generated {num_orders} test orders'}
            
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def get_performance_metrics(self):
        """Get database and query performance metrics"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Query execution times (simplified - in production, use EXPLAIN QUERY PLAN)
        queries = {
            'menu_retrieval': 'SELECT COUNT(*) FROM menu_view',
            'order_lookup': 'SELECT COUNT(*) FROM orders WHERE customer_id = 1',
            'delivery_assignment': '''
                SELECT COUNT(*) FROM delivery_persons dp 
                JOIN area_coverage ac ON dp.driver_id = ac.driver_id 
                WHERE ac.postal_code = "6211" AND dp.is_available = 1
            ''',
            'revenue_calculation': 'SELECT SUM(total_amount) FROM orders WHERE order_date >= date("now", "-30 days")'
        }
        
        performance = {}
        for query_name, query in queries.items():
            start_time = datetime.now()
            cursor.execute(query)
            cursor.fetchall()  # Ensure query executes
            end_time = datetime.now()
            performance[query_name] = (end_time - start_time).total_seconds()
        
        # Index usage information
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type = 'index' AND name NOT LIKE 'sqlite_%'
        ''')
        indexes = [row[0] for row in cursor.fetchall()]
        
        # Table sizes
        cursor.execute('''
            SELECT name FROM sqlite_master WHERE type = 'table'
        ''')
        tables = cursor.fetchall()
        
        table_sizes = {}
        for table in tables:
            cursor.execute(f'SELECT COUNT(*) FROM {table[0]}')
            table_sizes[table[0]] = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'query_performance': performance,
            'indexes': indexes,
            'table_sizes': table_sizes
        }
    