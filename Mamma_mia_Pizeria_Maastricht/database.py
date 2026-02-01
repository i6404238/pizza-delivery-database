import sqlite3
import datetime
from datetime import date, timedelta

class Database:
    def __init__(self, db_name="pizza_shop.db"):
        self.db_name = db_name
        self.init_database()
    
    def get_connection(self):
        return sqlite3.connect(self.db_name)
    
    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute('PRAGMA foreign_keys = ON')

        # Set timezone to Europe/Amsterdam for the connection
        cursor.execute("PRAGMA timezone = 'Europe/Amsterdam'")
        
        # Drop tables if they exist (for development)
        cursor.executescript('''
            DROP TABLE IF EXISTS order_cancellations;
            DROP TABLE IF EXISTS order_items;
            DROP TABLE IF EXISTS pizza_ingredients;
            DROP TABLE IF EXISTS area_coverage;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS discount_codes;
            DROP TABLE IF EXISTS customers;
            DROP TABLE IF EXISTS pizzas;
            DROP TABLE IF EXISTS ingredients;
            DROP TABLE IF EXISTS drinks;
            DROP TABLE IF EXISTS desserts;
            DROP TABLE IF EXISTS delivery_persons;
            
            -- Drop triggers if they exist
            DROP TRIGGER IF EXISTS check_vegetarian_pizza_insert;
            DROP TRIGGER IF EXISTS check_vegetarian_pizza_update;
            DROP TRIGGER IF EXISTS validate_customer_age_insert;
            DROP TRIGGER IF EXISTS validate_customer_age_update;
            DROP TRIGGER IF EXISTS prevent_discount_reuse;
            DROP TRIGGER IF EXISTS validate_order_has_pizza;
            DROP TRIGGER IF EXISTS update_pizza_vegetarian_status_insert;
            DROP TRIGGER IF EXISTS update_pizza_vegetarian_status_delete;
            DROP TRIGGER IF EXISTS update_pizza_vegetarian_status_update;
            DROP TRIGGER IF EXISTS prevent_late_cancellation;
            DROP TRIGGER IF EXISTS ensure_unique_discount_codes;
            DROP TRIGGER IF EXISTS validate_order_total;
        ''')
        
        # Create tables with enhanced constraints
        cursor.executescript('''
            -- Customers table with age validation
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT NOT NULL,
                address TEXT NOT NULL,
                postal_code TEXT NOT NULL CHECK(length(postal_code) >= 4),
                birth_date DATE NOT NULL,
                gender TEXT CHECK(gender IN ('Male', 'Female', 'Other')) NOT NULL,
                total_pizzas_ordered INTEGER DEFAULT 0 CHECK(total_pizzas_ordered >= 0),
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Ingredients table with strict cost validation
            CREATE TABLE ingredients (
                ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                cost DECIMAL(10,2) NOT NULL CHECK(cost > 0 AND cost < 100),
                is_vegetarian BOOLEAN NOT NULL DEFAULT 0 CHECK(is_vegetarian IN (0, 1)),
                is_vegan BOOLEAN NOT NULL DEFAULT 0 CHECK(is_vegan IN (0, 1))
            );
            
            -- Pizzas table
            CREATE TABLE pizzas (
                pizza_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                size TEXT NOT NULL CHECK(size IN ('Small', 'Medium', 'Large')) DEFAULT 'Medium',
                category TEXT NOT NULL CHECK(category IN ('Classic', 'Specialty', 'Premium')) DEFAULT 'Classic',
                is_vegetarian BOOLEAN NOT NULL DEFAULT 0 CHECK(is_vegetarian IN (0, 1))
            );
            
            -- Pizza-Ingredients junction table with quantity validation
            CREATE TABLE pizza_ingredients (
                pizza_id INTEGER NOT NULL,
                ingredient_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity <= 10) DEFAULT 1,
                PRIMARY KEY (pizza_id, ingredient_id),
                FOREIGN KEY (pizza_id) REFERENCES pizzas(pizza_id) ON DELETE CASCADE,
                FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id) ON DELETE CASCADE
            );
            
            -- Drinks table
            CREATE TABLE drinks (
                drink_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL CHECK(price > 0 AND price < 50),
                size TEXT NOT NULL CHECK(size IN ('Small', 'Medium', 'Large')) DEFAULT 'Medium'
            );
            
            -- Desserts table
            CREATE TABLE desserts (
                dessert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL CHECK(price > 0 AND price < 50)
            );
            
            -- Delivery persons table
            CREATE TABLE delivery_persons (
                driver_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                is_available BOOLEAN NOT NULL DEFAULT 1 CHECK(is_available IN (0, 1)),
                last_delivery_time TIMESTAMP,
                current_location TEXT,
                vehicle_type TEXT NOT NULL CHECK(vehicle_type IN ('Bike', 'Scooter', 'Car')) DEFAULT 'Bike'
            );
            
            -- Area coverage table
            CREATE TABLE area_coverage (
                driver_id INTEGER NOT NULL,
                postal_code TEXT NOT NULL CHECK(length(postal_code) >= 4),
                area_name TEXT NOT NULL,
                delivery_time_minutes INTEGER NOT NULL CHECK(delivery_time_minutes > 0 AND delivery_time_minutes <= 120) DEFAULT 25,
                PRIMARY KEY (driver_id, postal_code),
                FOREIGN KEY (driver_id) REFERENCES delivery_persons(driver_id) ON DELETE CASCADE
            );
            
            -- Discount codes table with strict constraints
            CREATE TABLE discount_codes (
                code_id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL CHECK(length(code) >= 4),
                discount_percent INTEGER NOT NULL CHECK(discount_percent BETWEEN 1 AND 100),
                is_used BOOLEAN NOT NULL DEFAULT 0 CHECK(is_used IN (0, 1)),
                used_by_customer_id INTEGER,
                expiry_date DATE NOT NULL,
                FOREIGN KEY (used_by_customer_id) REFERENCES customers(customer_id)
            );
            
            -- Orders table with comprehensive constraints
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL CHECK(total_amount >= 0 AND total_amount < 1000),
                status TEXT NOT NULL CHECK(status IN ('Pending', 'Preparing', 'Out for Delivery', 'Delivered', 'Cancelled')) DEFAULT 'Pending',
                delivery_person_id INTEGER,
                discount_applied DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK(discount_applied >= 0),
                estimated_delivery_time TIMESTAMP,
                actual_delivery_time TIMESTAMP,
                delivery_notes TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (delivery_person_id) REFERENCES delivery_persons(driver_id),
                CHECK (actual_delivery_time IS NULL OR actual_delivery_time >= order_date),
                CHECK (estimated_delivery_time IS NULL OR estimated_delivery_time >= order_date)
            );
            
            -- Order items table with strict validation
            CREATE TABLE order_items (
                order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                item_type TEXT NOT NULL CHECK(item_type IN ('pizza', 'drink', 'dessert')),
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity <= 20),
                price_at_time DECIMAL(10,2) NOT NULL CHECK(price_at_time > 0),
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            );
            
            -- Order cancellations table for tracking cancellations
            CREATE TABLE order_cancellations (
                cancellation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                cancelled_by TEXT NOT NULL,
                cancellation_type TEXT NOT NULL CHECK(cancellation_type IN ('customer', 'staff')),
                cancellation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reason TEXT,
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            );
        ''')
        
        # Create custom constraints and triggers
        self.create_constraints_and_triggers(cursor)
        
        # Insert sample data
        self.insert_sample_data(cursor)
        
        # Create views
        self.create_views(cursor)
        
        conn.commit()
        conn.close()
    
    def create_constraints_and_triggers(self, cursor):
        """Create custom constraints and triggers for business rules"""
        
        # Trigger 1: Ensure vegetarian pizzas don't contain meat
        cursor.execute('''
            CREATE TRIGGER check_vegetarian_pizza_insert
            BEFORE INSERT ON pizza_ingredients
            FOR EACH ROW
            WHEN (
                SELECT is_vegetarian FROM pizzas WHERE pizza_id = NEW.pizza_id
            ) = 1
            BEGIN
                SELECT CASE
                    WHEN (SELECT is_vegetarian FROM ingredients WHERE ingredient_id = NEW.ingredient_id) = 0
                    THEN RAISE(ABORT, 'Vegetarian pizza cannot contain non-vegetarian ingredients')
                END;
            END;
        ''')
        
        cursor.execute('''
            CREATE TRIGGER check_vegetarian_pizza_update
            BEFORE UPDATE ON pizza_ingredients
            FOR EACH ROW
            WHEN (
                SELECT is_vegetarian FROM pizzas WHERE pizza_id = NEW.pizza_id
            ) = 1
            BEGIN
                SELECT CASE
                    WHEN (SELECT is_vegetarian FROM ingredients WHERE ingredient_id = NEW.ingredient_id) = 0
                    THEN RAISE(ABORT, 'Vegetarian pizza cannot contain non-vegetarian ingredients')
                END;
            END;
        ''')
        
        # Trigger 2: Validate customer age (at least 13 years old)
        cursor.execute('''
            CREATE TRIGGER validate_customer_age_insert
            BEFORE INSERT ON customers
            FOR EACH ROW
            BEGIN
                SELECT CASE
                    WHEN date(NEW.birth_date) > date('now', '-13 years')
                    THEN RAISE(ABORT, 'Customer must be at least 13 years old')
                END;
            END;
        ''')
        
        cursor.execute('''
            CREATE TRIGGER validate_customer_age_update
            BEFORE UPDATE ON customers
            FOR EACH ROW
            BEGIN
                SELECT CASE
                    WHEN date(NEW.birth_date) > date('now', '-13 years')
                    THEN RAISE(ABORT, 'Customer must be at least 13 years old')
                END;
            END;
        ''')
        
        # Trigger 3: Prevent discount code reuse
        cursor.execute('''
            CREATE TRIGGER prevent_discount_reuse
            BEFORE UPDATE ON discount_codes
            FOR EACH ROW
            WHEN NEW.is_used = 1 AND OLD.is_used = 0
            BEGIN
                -- Check if this code is already used by someone else
                SELECT CASE
                    WHEN EXISTS (
                        SELECT 1 FROM discount_codes 
                        WHERE code = NEW.code AND is_used = 1 AND code_id != NEW.code_id
                    )
                    THEN RAISE(ABORT, 'Discount code has already been used')
                END;
            END;
        ''')
        
        # Trigger 4: Ensure at least one pizza per order - FIXED: Use CASE instead of IF
        cursor.execute('''
            CREATE TRIGGER validate_order_has_pizza
            AFTER INSERT ON order_items
            FOR EACH ROW
            WHEN (SELECT COUNT(*) FROM order_items WHERE order_id = NEW.order_id AND item_type = 'pizza') = 0
            BEGIN
                -- Delete the invalid order
                DELETE FROM orders WHERE order_id = NEW.order_id;
                SELECT RAISE(ABORT, 'Order must contain at least one pizza');
            END;
        ''')
        
        # Trigger 5: Update pizza vegetarian status automatically
        cursor.execute('''
            CREATE TRIGGER update_pizza_vegetarian_status_insert
            AFTER INSERT ON pizza_ingredients
            FOR EACH ROW
            BEGIN
                UPDATE pizzas 
                SET is_vegetarian = (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN 0
                        WHEN SUM(CASE WHEN i.is_vegetarian = 0 THEN 1 ELSE 0 END) = 0 THEN 1
                        ELSE 0
                    END
                    FROM pizza_ingredients pi
                    JOIN ingredients i ON pi.ingredient_id = i.ingredient_id
                    WHERE pi.pizza_id = NEW.pizza_id
                )
                WHERE pizza_id = NEW.pizza_id;
            END;
        ''')
        
        cursor.execute('''
            CREATE TRIGGER update_pizza_vegetarian_status_delete
            AFTER DELETE ON pizza_ingredients
            FOR EACH ROW
            BEGIN
                UPDATE pizzas 
                SET is_vegetarian = (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN 0
                        WHEN SUM(CASE WHEN i.is_vegetarian = 0 THEN 1 ELSE 0 END) = 0 THEN 1
                        ELSE 0
                    END
                    FROM pizza_ingredients pi
                    JOIN ingredients i ON pi.ingredient_id = i.ingredient_id
                    WHERE pi.pizza_id = OLD.pizza_id
                )
                WHERE pizza_id = OLD.pizza_id;
            END;
        ''')
        
        cursor.execute('''
            CREATE TRIGGER update_pizza_vegetarian_status_update
            AFTER UPDATE ON pizza_ingredients
            FOR EACH ROW
            BEGIN
                UPDATE pizzas 
                SET is_vegetarian = (
                    SELECT CASE 
                        WHEN COUNT(*) = 0 THEN 0
                        WHEN SUM(CASE WHEN i.is_vegetarian = 0 THEN 1 ELSE 0 END) = 0 THEN 1
                        ELSE 0
                    END
                    FROM pizza_ingredients pi
                    JOIN ingredients i ON pi.ingredient_id = i.ingredient_id
                    WHERE pi.pizza_id = NEW.pizza_id
                )
                WHERE pizza_id = NEW.pizza_id;
            END;
        ''')
        
        # Trigger 6: Prevent order cancellation after 5 minutes
        cursor.execute('''
            CREATE TRIGGER prevent_late_cancellation
            BEFORE UPDATE OF status ON orders
            FOR EACH ROW
            WHEN NEW.status = 'Cancelled' AND OLD.status != 'Cancelled'
            BEGIN
                SELECT CASE
                    WHEN datetime('now') > datetime(OLD.order_date, '+5 minutes')
                    THEN RAISE(ABORT, 'Orders can only be cancelled within 5 minutes of placement')
                END;
            END;
        ''')
        
        # Trigger 7: Ensure unique discount codes (enhanced)
        cursor.execute('''
            CREATE TRIGGER ensure_unique_discount_codes
            BEFORE INSERT ON discount_codes
            FOR EACH ROW
            BEGIN
                SELECT CASE
                    WHEN EXISTS (SELECT 1 FROM discount_codes WHERE code = NEW.code)
                    THEN RAISE(ABORT, 'Discount code must be unique')
                END;
            END;
        ''')
        
        # Trigger 8: Validate order total amount
        cursor.execute('''
            CREATE TRIGGER validate_order_total
            BEFORE INSERT ON orders
            FOR EACH ROW
            BEGIN
                SELECT CASE
                    WHEN NEW.total_amount < 0 
                    THEN RAISE(ABORT, 'Order total cannot be negative')
                    WHEN NEW.total_amount > 1000
                    THEN RAISE(ABORT, 'Order total cannot exceed 1000')
                END;
            END;
        ''')
        
        # Create indexes for performance
        cursor.executescript('''
            CREATE INDEX IF NOT EXISTS idx_orders_customer_date ON orders(customer_id, order_date);
            CREATE INDEX IF NOT EXISTS idx_orders_status_date ON orders(status, order_date);
            CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
            CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
            CREATE INDEX IF NOT EXISTS idx_pizza_ingredients_pizza_id ON pizza_ingredients(pizza_id);
            CREATE INDEX IF NOT EXISTS idx_area_coverage_postal_code ON area_coverage(postal_code);
            CREATE INDEX IF NOT EXISTS idx_order_cancellations_order_id ON order_cancellations(order_id);
            CREATE INDEX IF NOT EXISTS idx_orders_delivery_person ON orders(delivery_person_id);
            CREATE INDEX IF NOT EXISTS idx_customers_postal_code ON customers(postal_code);
        ''')

    def insert_sample_data(self, cursor):    
        # Disable the vegetarian check triggers temporarily
        cursor.execute('DROP TRIGGER IF EXISTS check_vegetarian_pizza_insert')
        cursor.execute('DROP TRIGGER IF EXISTS check_vegetarian_pizza_update')
        
        local_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert ingredients with valid costs
        ingredients = [
            ('Tomato Sauce', 0.5, 1, 1),
            ('Mozzarella Cheese', 1.2, 1, 0),
            ('Pepperoni', 1.5, 0, 0),
            ('Mushrooms', 0.8, 1, 1),
            ('Green Peppers', 0.7, 1, 1),
            ('Onions', 0.6, 1, 1),
            ('Black Olives', 0.9, 1, 1),
            ('Sausage', 1.4, 0, 0),
            ('Bacon', 1.6, 0, 0),
            ('Ham', 1.3, 0, 0),
            ('Pineapple', 0.9, 1, 1),
            ('Fresh Basil', 0.5, 1, 1),
            ('Vegan Cheese', 1.8, 1, 1)
        ]
        cursor.executemany(
            'INSERT INTO ingredients (name, cost, is_vegetarian, is_vegan) VALUES (?, ?, ?, ?)',
            ingredients
        )

        # Insert pizzas (set all as non-vegetarian initially)
        pizzas = [
            ('Margherita', 'Classic tomato and cheese', 'Medium', 'Classic', 0),
            ('Pepperoni', 'Pepperoni and cheese', 'Medium', 'Classic', 0),
            ('Vegetarian', 'Mixed vegetables', 'Medium', 'Classic', 0),
            ('Hawaiian', 'Ham and pineapple', 'Medium', 'Classic', 0),
            ('Meat Lovers', 'All meat toppings', 'Large', 'Premium', 0),
            ('Vegan Special', 'Vegan cheese and vegetables', 'Medium', 'Specialty', 0),
            ('BBQ Chicken', 'BBQ sauce with chicken', 'Large', 'Premium', 0),
            ('Four Cheese', 'Four cheese blend', 'Medium', 'Classic', 0),
            ('Mushroom Delight', 'Extra mushrooms', 'Medium', 'Classic', 0),
            ('Spicy Italian', 'Spicy sausage and peppers', 'Medium', 'Specialty', 0)
        ]
        cursor.executemany(
            'INSERT INTO pizzas (name, description, size, category, is_vegetarian) VALUES (?, ?, ?, ?, ?)',
            pizzas
        )

        # Insert pizza ingredients - now all should work
        pizza_ingredients = [
            # Margherita
            (1, 1), (1, 2), (1, 12),
            # Pepperoni
            (2, 1), (2, 2), (2, 3),
            # Vegetarian
            (3, 1), (3, 2), (3, 4), (3, 5), (3, 6), (3, 7),
            # Hawaiian
            (4, 1), (4, 2), (4, 10), (4, 11),
            # Meat Lovers
            (5, 1), (5, 2), (5, 3), (5, 8), (5, 9),
            # Vegan Special
            (6, 1), (6, 13), (6, 4), (6, 5), (6, 6),
            # BBQ Chicken
            (7, 1), (7, 2), (7, 8),
            # Four Cheese
            (8, 1), (8, 2), (8, 13),
            # Mushroom Delight
            (9, 1), (9, 2), (9, 4),
            # Spicy Italian
            (10, 1), (10, 2), (10, 8), (10, 5)
        ]

        cursor.executemany(
            'INSERT INTO pizza_ingredients (pizza_id, ingredient_id) VALUES (?, ?)',
            pizza_ingredients
        )

        # The rest of your insert methods remain the same...
        # Insert drinks
        drinks = [
            ('Coca-Cola', 2.5, 'Medium'),
            ('Sprite', 2.5, 'Medium'),
            ('Water', 1.5, 'Medium'),
            ('Orange Juice', 3.0, 'Medium'),
            ('Beer', 4.5, 'Medium')
        ]
        cursor.executemany(
            'INSERT INTO drinks (name, price, size) VALUES (?, ?, ?)',
            drinks
        )

        # Insert desserts
        desserts = [
            ('Tiramisu', 4.5),
            ('Chocolate Cake', 3.5),
            ('Ice Cream', 3.0),
            ('Cheesecake', 4.0)
        ]
        cursor.executemany(
            'INSERT INTO desserts (name, price) VALUES (?, ?)',
            desserts
        )

        # Insert delivery persons
        delivery_persons = [
            ('Jan de Vries', '+31 6 12345678', 1, None, 'City Center', 'Bike'),
            ('Lisa Jansen', '+31 6 23456789', 1, None, 'Wyck', 'Scooter'),
            ('Mohammed Ali', '+31 6 34567890', 1, None, 'Caberg', 'Car'),
            ('Anna Schmidt', '+31 6 45678901', 1, None, 'Malberg', 'Bike'),
            ('Tom Bakker', '+31 6 56789012', 1, None, 'Brusselsepoort', 'Scooter')
        ]
        cursor.executemany(
            'INSERT INTO delivery_persons (name, phone, is_available, last_delivery_time, current_location, vehicle_type) VALUES (?, ?, ?, ?, ?, ?)',
            delivery_persons
        )

        # Insert area coverage
        area_coverage = [
            (1, '6211', 'City Center - Markt', 15),
            (1, '6212', 'City Center - Vrijthof', 15),
            (1, '6213', 'City Center - Boschstraat', 18),
            (2, '6221', 'Wyck - Station', 20),
            (2, '6222', 'Wyck - Maasboulevard', 22),
            (2, '6223', 'Wyck - Rechtstraat', 25),
            (3, '6217', 'Caberg', 30),
            (3, '6218', 'Daalhof', 28),
            (3, '6219', 'Mariaberg', 35),
            (4, '6215', 'Malberg', 25),
            (4, '6216', 'Malpertuis', 28),
            (4, '6224', 'Nazareth', 32),
            (5, '6214', 'Brusselsepoort', 22),
            (5, '6225', 'Amby', 35),
            (5, '6226', 'Heugem', 30),
            (5, '6227', 'Randwyck', 25),
            (5, '6228', 'Heer', 40),
            (5, '6229', 'Borgharen', 45)
        ]
        cursor.executemany(
            'INSERT INTO area_coverage (driver_id, postal_code, area_name, delivery_time_minutes) VALUES (?, ?, ?, ?)',
            area_coverage
        )

        # Insert discount codes with future expiry dates
        future_date = (datetime.datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        discount_codes = [
            ('WELCOME10', 10, 0, None, future_date),
            ('PIZZALOVER', 15, 0, None, future_date),
            ('SAVE20', 20, 0, None, future_date),
            ('MAASTRICHT25', 25, 0, None, future_date)
        ]
        cursor.executemany(
            'INSERT INTO discount_codes (code, discount_percent, is_used, used_by_customer_id, expiry_date) VALUES (?, ?, ?, ?, ?)',
            discount_codes
        )

        # Insert sample customers with valid birth dates (at least 13 years old)
        customers = [
            ('Emma van Dijk', 'emma@email.com', '+31 6 11111111', 'Vrijthof 25', '6211', '1990-05-15', 'Female', 5),
            ('Lucas Jansen', 'lucas@email.com', '+31 6 22222222', 'Stationstraat 12', '6221', '1985-08-22', 'Male', 12),
            ('Sophie de Wit', 'sophie@email.com', '+31 6 33333333', 'Cabergerweg 8', '6217', '1995-02-10', 'Female', 8),
            ('Thomas Maas', 'thomas@email.com', '+31 6 44444444', 'Malbergweg 15', '6215', '1988-11-30', 'Male', 3),
            ('Isabelle Horn', 'isabelle@email.com', '+31 6 55555555', 'Brusselsepoort 42', '6214', '1992-07-18', 'Female', 7)
        ]
        cursor.executemany(
            'INSERT INTO customers (name, email, phone, address, postal_code, birth_date, gender, total_pizzas_ordered) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            customers
        )

        # Re-create the vegetarian check triggers after sample data is inserted
        cursor.execute('''
            CREATE TRIGGER check_vegetarian_pizza_insert
            BEFORE INSERT ON pizza_ingredients
            FOR EACH ROW
            WHEN (
                SELECT is_vegetarian FROM pizzas WHERE pizza_id = NEW.pizza_id
            ) = 1
            BEGIN
                SELECT CASE
                    WHEN (SELECT is_vegetarian FROM ingredients WHERE ingredient_id = NEW.ingredient_id) = 0
                    THEN RAISE(ABORT, 'Vegetarian pizza cannot contain non-vegetarian ingredients')
                END;
            END;
        ''')

        cursor.execute('''
            CREATE TRIGGER check_vegetarian_pizza_update
            BEFORE UPDATE ON pizza_ingredients
            FOR EACH ROW
            WHEN (
                SELECT is_vegetarian FROM pizzas WHERE pizza_id = NEW.pizza_id
            ) = 1
            BEGIN
                SELECT CASE
                    WHEN (SELECT is_vegetarian FROM ingredients WHERE ingredient_id = NEW.ingredient_id) = 0
                    THEN RAISE(ABORT, 'Vegetarian pizza cannot contain non-vegetarian ingredients')
                END;
            END;
        ''')

        
    
    def create_views(self, cursor):
        # View for pizza prices with dynamic calculation
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS pizza_prices AS
            SELECT 
                p.pizza_id,
                p.name,
                p.size,
                p.category,
                SUM(i.cost) as base_cost,
                ROUND((SUM(i.cost) * 1.4) * 1.09, 2) as final_price,
                p.is_vegetarian,
                CASE 
                    WHEN SUM(CASE WHEN i.is_vegan = 0 THEN 1 ELSE 0 END) = 0 THEN 1 
                    ELSE 0 
                END as is_vegan
            FROM pizzas p
            JOIN pizza_ingredients pi ON p.pizza_id = pi.pizza_id
            JOIN ingredients i ON pi.ingredient_id = i.ingredient_id
            GROUP BY p.pizza_id
        ''')
        
        # View for menu items
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS menu_view AS
            SELECT 
                'pizza' as item_type,
                pizza_id as item_id,
                name,
                final_price as price,
                size,
                is_vegetarian,
                is_vegan
            FROM pizza_prices
            UNION ALL
            SELECT 
                'drink' as item_type,
                drink_id as item_id,
                name,
                price,
                size,
                0 as is_vegetarian,
                0 as is_vegan
            FROM drinks
            UNION ALL
            SELECT 
                'dessert' as item_type,
                dessert_id as item_id,
                name,
                price,
                '' as size,
                0 as is_vegetarian,
                0 as is_vegan
            FROM desserts
        ''')
        
        # View for delivery assignments
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS delivery_assignments AS
            SELECT 
                o.order_id,
                o.customer_id,
                c.name as customer_name,
                c.address,
                c.postal_code,
                o.total_amount,
                o.status,
                o.delivery_person_id,
                dp.name as delivery_person_name,
                dp.phone as delivery_phone,
                dp.vehicle_type,
                ac.area_name,
                ac.delivery_time_minutes,
                o.estimated_delivery_time,
                o.actual_delivery_time,
                o.order_date
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN delivery_persons dp ON o.delivery_person_id = dp.driver_id
            LEFT JOIN area_coverage ac ON (dp.driver_id = ac.driver_id AND c.postal_code = ac.postal_code)
        ''')

        # View for order cancellation eligibility
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS order_cancellation_eligibility AS
            SELECT 
                order_id,
                customer_id,
                order_date,
                status,
                CASE 
                    WHEN datetime('now') <= datetime(order_date, '+5 minutes') 
                    THEN 1 
                    ELSE 0 
                END as can_cancel,
                datetime(order_date, '+5 minutes') as cancellation_deadline
            FROM orders
            WHERE status IN ('Pending', 'Preparing')
        ''')

        # View for real-time inventory tracking
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS ingredient_usage AS
            SELECT 
                i.ingredient_id,
                i.name,
                i.cost,
                COALESCE(SUM(pi.quantity * oi.quantity), 0) as total_used,
                i.cost * COALESCE(SUM(pi.quantity * oi.quantity), 0) as total_cost
            FROM ingredients i
            LEFT JOIN pizza_ingredients pi ON i.ingredient_id = pi.ingredient_id
            LEFT JOIN order_items oi ON pi.pizza_id = oi.item_id AND oi.item_type = 'pizza'
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date >= date('now', '-7 days')
            GROUP BY i.ingredient_id, i.name, i.cost
        ''')

        # View for customer loyalty tiers
        cursor.execute('''
            CREATE VIEW IF NOT EXISTS customer_loyalty_tiers AS
            SELECT 
                customer_id,
                name,
                total_pizzas_ordered,
                CASE 
                    WHEN total_pizzas_ordered >= 20 THEN 'Gold'
                    WHEN total_pizzas_ordered >= 10 THEN 'Silver'
                    WHEN total_pizzas_ordered >= 5 THEN 'Bronze'
                    ELSE 'New'
                END as loyalty_tier,
                CASE 
                    WHEN total_pizzas_ordered >= 20 THEN 15
                    WHEN total_pizzas_ordered >= 10 THEN 10
                    WHEN total_pizzas_ordered >= 5 THEN 5
                    ELSE 0
                END as loyalty_discount_percent
            FROM customers
            WHERE total_pizzas_ordered > 0
        ''')